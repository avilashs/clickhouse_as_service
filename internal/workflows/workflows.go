// Package workflows holds the durable cluster lifecycle workflows (ADR-0001). A workflow's ID
// is "cluster/<id>", so Temporal itself guarantees at most one running mutation per cluster;
// the operations_one_active index enforces the same at the database layer.
package workflows

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

const TaskQueue = "basalt"

// Input identifies the operation a workflow executes. Everything else is read from the store.
type Input struct {
	OperationID string
	ClusterID   string
}

// Timeouts, overridable in tests.
var (
	ConvergeTimeout = 20 * time.Minute
	DeleteTimeout   = 10 * time.Minute
)

var retry = &temporal.RetryPolicy{
	InitialInterval:        time.Second,
	BackoffCoefficient:     2,
	MaximumInterval:        30 * time.Second,
	NonRetryableErrorTypes: []string{ErrTypeCapacity, ErrTypeInvalid},
}

func shortOpts(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: retry})
}

func waitOpts(ctx workflow.Context, d time.Duration) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: d,
		HeartbeatTimeout:    20 * time.Second, // a killed worker is detected this fast
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: 10 * time.Second,
			NonRetryableErrorTypes: []string{ErrTypeTimeout, ErrTypeInvalid},
			MaximumAttempts:        0,
		},
	})
}

var a *Activities // method expressions only; never dereferenced in workflow code

// CreateCluster: reserve capacity → credentials → publish desired state → wait until the
// agent reports the cluster ready → commit. On failure after provisioning started, it
// compensates: tombstone the desired state, wait for teardown, release capacity, mark FAILED.
func CreateCluster(ctx workflow.Context, in Input) error {
	sctx := shortOpts(ctx)
	if err := workflow.ExecuteActivity(sctx, a.MarkOperationRunning, in.OperationID).Get(ctx, nil); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(sctx, a.ReserveCapacity, ReserveInput{in.OperationID, in.ClusterID}).Get(ctx, nil); err != nil {
		// nothing was provisioned: fail without compensation
		_ = workflow.ExecuteActivity(sctx, a.FailOperation, FailInput{in.OperationID, in.ClusterID, errMessage(err), true}).Get(ctx, nil)
		return err
	}
	var gen int64
	err := workflow.ExecuteActivity(sctx, a.EnsureClusterSecrets, in.ClusterID).Get(ctx, nil)
	if err == nil {
		err = workflow.ExecuteActivity(sctx, a.PublishDesired, PublishInput{ClusterID: in.ClusterID}).Get(ctx, &gen)
	}
	if err == nil {
		var h engine.Health
		err = workflow.ExecuteActivity(waitOpts(ctx, ConvergeTimeout), a.WaitForPhase,
			WaitInput{ClusterID: in.ClusterID, Generation: gen, Want: engine.PhaseReady}).Get(ctx, &h)
	}
	if err != nil {
		compensate(ctx, in, errMessage(err))
		return err
	}
	return workflow.ExecuteActivity(sctx, a.FinishCreate, FinishInput{in.OperationID, in.ClusterID}).Get(ctx, nil)
}

// compensate tears down whatever a failed create provisioned. It runs on a disconnected
// context so that it completes even if the workflow is being cancelled.
func compensate(ctx workflow.Context, in Input, reason string) {
	dctx, _ := workflow.NewDisconnectedContext(ctx)
	sctx := shortOpts(dctx)
	var gen int64
	released := false
	if err := workflow.ExecuteActivity(sctx, a.PublishDesired, PublishInput{ClusterID: in.ClusterID, Tombstone: true}).Get(dctx, &gen); err == nil {
		var h engine.Health
		if err := workflow.ExecuteActivity(waitOpts(dctx, DeleteTimeout), a.WaitForPhase,
			WaitInput{ClusterID: in.ClusterID, Generation: gen, Want: engine.PhaseGone}).Get(dctx, &h); err == nil {
			released = true
		}
	}
	msg := "create failed: " + reason
	if !released {
		msg += " (teardown not confirmed; capacity stays reserved until the cluster is deleted)"
	}
	_ = workflow.ExecuteActivity(sctx, a.FailOperation, FailInput{in.OperationID, in.ClusterID, msg, released}).Get(dctx, nil)
}

// DeleteCluster: mark DELETING → tombstone → wait until the agent reports the namespace gone →
// release capacity, delete credentials, soft-delete the record.
func DeleteCluster(ctx workflow.Context, in Input) error {
	sctx := shortOpts(ctx)
	if err := workflow.ExecuteActivity(sctx, a.MarkOperationRunning, in.OperationID).Get(ctx, nil); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(sctx, a.MarkDeleting, in.ClusterID).Get(ctx, nil); err != nil {
		return err
	}
	var gen int64
	if err := workflow.ExecuteActivity(sctx, a.PublishDesired, PublishInput{ClusterID: in.ClusterID, Tombstone: true}).Get(ctx, &gen); err != nil {
		return err
	}
	var h engine.Health
	if err := workflow.ExecuteActivity(waitOpts(ctx, DeleteTimeout), a.WaitForPhase,
		WaitInput{ClusterID: in.ClusterID, Generation: gen, Want: engine.PhaseGone}).Get(ctx, &h); err != nil {
		_ = workflow.ExecuteActivity(sctx, a.FailOperation, FailInput{in.OperationID, in.ClusterID, "delete did not complete: " + errMessage(err), false}).Get(ctx, nil)
		return err
	}
	return workflow.ExecuteActivity(sctx, a.FinishDelete, FinishInput{in.OperationID, in.ClusterID}).Get(ctx, nil)
}

func errMessage(err error) string {
	var app *temporal.ApplicationError
	if errors.As(err, &app) {
		return app.Error()
	}
	return err.Error()
}

// WorkflowFor maps an operation kind to its workflow function.
func WorkflowFor(kind model.OperationKind) (any, error) {
	switch kind {
	case model.OpCreateCluster:
		return CreateCluster, nil
	case model.OpDeleteCluster:
		return DeleteCluster, nil
	}
	return nil, fmt.Errorf("no workflow for %s", kind)
}

// Start starts (or attaches to) the workflow for an operation. It is idempotent: if the
// workflow for this cluster is already running, the existing run is used.
func Start(ctx context.Context, c client.Client, op model.Operation) error {
	wf, err := WorkflowFor(op.Kind)
	if err != nil {
		return err
	}
	_, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       "cluster/" + op.ClusterID,
		TaskQueue:                TaskQueue,
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		Memo:                     map[string]any{"operationId": op.ID},
	}, wf, Input{OperationID: op.ID, ClusterID: op.ClusterID})
	return err
}
