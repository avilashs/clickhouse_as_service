package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/engine/clickhouse"
	"github.com/avilashs/clickhouse_as_service/internal/model"
	"github.com/avilashs/clickhouse_as_service/internal/secrets"
	"github.com/avilashs/clickhouse_as_service/internal/store"
)

// Non-retryable error types: the workflow fails (and compensates) instead of retrying.
const (
	ErrTypeCapacity = "Capacity"
	ErrTypeInvalid  = "Invalid"
	ErrTypeTimeout  = "ConvergenceTimeout"
)

// Activities holds the dependencies of every activity. All activities are idempotent: each
// one can be retried after a worker crash at any point without creating duplicates.
type Activities struct {
	Store   *store.Store
	Secrets secrets.Store
	Engines map[string]engine.Engine
	// PollInterval for convergence waits (tests shorten it).
	PollInterval time.Duration
}

func (a *Activities) engineFor(c model.Cluster) (engine.Engine, error) {
	e, ok := a.Engines[c.Spec.Engine]
	if !ok {
		return nil, temporal.NewNonRetryableApplicationError("unknown engine "+c.Spec.Engine, ErrTypeInvalid, nil)
	}
	return e, nil
}

func (a *Activities) MarkOperationRunning(ctx context.Context, opID string) error {
	return a.Store.SetOperationState(ctx, opID, model.OpRunning, "")
}

type ReserveInput struct{ OperationID, ClusterID string }

// ReserveCapacity holds capacity in the cluster's cell before anything is provisioned.
func (a *Activities) ReserveCapacity(ctx context.Context, in ReserveInput) error {
	c, err := a.Store.GetCluster(ctx, "", in.ClusterID)
	if err != nil {
		return err
	}
	e, err := a.engineFor(c)
	if err != nil {
		return err
	}
	plan, err := e.PlanPlacement(c.Spec)
	if err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeInvalid, err)
	}
	if err := a.Store.Reserve(ctx, in.OperationID, c.CellID, c.ID, plan); err != nil {
		if errors.Is(err, store.ErrCapacity) {
			return temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeCapacity, err)
		}
		return err
	}
	return nil
}

// EnsureClusterSecrets creates the cluster's credentials in the secret manager (get-or-create).
func (a *Activities) EnsureClusterSecrets(ctx context.Context, clusterID string) error {
	for _, name := range []string{
		clickhouse.DefaultPasswordSecret(clusterID),
		clickhouse.AdminPasswordSecret(clusterID),
	} {
		if _, err := a.Secrets.GetOrCreate(ctx, name, secrets.Password(32)); err != nil {
			return err
		}
	}
	return nil
}

type PublishInput struct {
	ClusterID string
	Tombstone bool
}

// PublishDesired renders the cluster and stores the payload for the cell agent. Rendering is
// pure and publishing an unchanged payload is a no-op, so retries return the same generation.
func (a *Activities) PublishDesired(ctx context.Context, in PublishInput) (int64, error) {
	c, err := a.Store.GetCluster(ctx, "", in.ClusterID)
	if err != nil {
		return 0, err
	}
	cell, _, err := a.Store.GetCell(ctx, c.CellID)
	if err != nil {
		return 0, err
	}
	var d engine.Desired
	if in.Tombstone {
		// a tombstone tells the agent to delete everything it created for the cluster
		d = engine.Desired{ClusterID: c.ID, Namespace: "ch-" + c.ID}
	} else {
		e, err := a.engineFor(c)
		if err != nil {
			return 0, err
		}
		d, err = e.Render(engine.RenderInput{Cluster: c, Cell: cell})
		if err != nil {
			return 0, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeInvalid, err)
		}
	}
	payload, err := json.Marshal(d)
	if err != nil {
		return 0, err
	}
	return a.Store.PublishDesired(ctx, c.ID, c.CellID, payload, d.Hash(), in.Tombstone)
}

type WaitInput struct {
	ClusterID  string
	Generation int64
	Want       engine.Phase // PhaseReady or PhaseGone
}

// WaitForPhase polls the agent's reports until the cluster reaches the wanted phase. It
// heartbeats, so a worker crash is detected within the heartbeat timeout and the wait
// resumes on another worker. The overall deadline is the activity's StartToClose timeout.
func (a *Activities) WaitForPhase(ctx context.Context, in WaitInput) (engine.Health, error) {
	interval := a.PollInterval
	if interval == 0 {
		interval = 3 * time.Second
	}
	var lastReason string
	for {
		h, err := a.health(ctx, in)
		if err != nil {
			return h, err
		}
		activity.RecordHeartbeat(ctx, h)
		if h.Phase == in.Want {
			return h, nil
		}
		if in.Want == engine.PhaseReady && len(h.Reasons) > 0 && h.Reasons[0] != lastReason {
			lastReason = h.Reasons[0]
			// progress is visible to the tenant while the cluster converges
			_ = a.Store.SetClusterState(ctx, in.ClusterID, model.ClusterCreating, truncate(lastReason, 300))
		}
		select {
		case <-ctx.Done():
			return h, temporal.NewApplicationError(fmt.Sprintf("cluster did not reach %s: %v", in.Want, h.Reasons), ErrTypeTimeout)
		case <-time.After(interval):
		}
	}
}

func (a *Activities) health(ctx context.Context, in WaitInput) (engine.Health, error) {
	obs, err := a.Store.GetObserved(ctx, in.ClusterID)
	if err != nil {
		return engine.Health{}, err
	}
	if in.Want == engine.PhaseGone {
		// gone = namespace deleted and, for dedicated clusters, its nodes returned to the cell
		if obs != nil && obs.AppliedGeneration >= in.Generation && !obs.NamespaceExists && obs.PoolNodes == 0 {
			return engine.Health{Phase: engine.PhaseGone}, nil
		}
		return engine.Health{Phase: engine.PhaseProgressing, Reasons: []string{"waiting for the agent to delete the namespace"}}, nil
	}
	row, err := a.Store.GetDesired(ctx, in.ClusterID)
	if err != nil {
		return engine.Health{}, err
	}
	var d engine.Desired
	if err := json.Unmarshal(row.Payload, &d); err != nil {
		return engine.Health{}, err
	}
	c, err := a.Store.GetCluster(ctx, "", in.ClusterID)
	if err != nil {
		return engine.Health{}, err
	}
	e, err := a.engineFor(c)
	if err != nil {
		return engine.Health{}, err
	}
	return e.Observe(d, in.Generation, obs), nil
}

type FinishInput struct{ OperationID, ClusterID string }

func (a *Activities) FinishCreate(ctx context.Context, in FinishInput) error {
	if err := a.Store.CommitReservation(ctx, in.OperationID); err != nil {
		return err
	}
	if err := a.Store.MarkClusterActive(ctx, in.ClusterID); err != nil {
		return err
	}
	_ = a.Store.Audit(ctx, "workflow", "cluster.created", in.ClusterID, in.OperationID, nil)
	return a.Store.SetOperationState(ctx, in.OperationID, model.OpSucceeded, "")
}

func (a *Activities) MarkDeleting(ctx context.Context, clusterID string) error {
	return a.Store.SetClusterState(ctx, clusterID, model.ClusterDeleting, "")
}

func (a *Activities) FinishDelete(ctx context.Context, in FinishInput) error {
	if err := a.Store.ReleaseClusterReservations(ctx, in.ClusterID); err != nil {
		return err
	}
	if err := a.Secrets.DeletePrefix(ctx, "clusters/"+in.ClusterID+"/"); err != nil {
		return err
	}
	if err := a.Store.ForgetDesired(ctx, in.ClusterID); err != nil {
		return err
	}
	if err := a.Store.MarkClusterDeleted(ctx, in.ClusterID); err != nil {
		return err
	}
	_ = a.Store.Audit(ctx, "workflow", "cluster.deleted", in.ClusterID, in.OperationID, nil)
	return a.Store.SetOperationState(ctx, in.OperationID, model.OpSucceeded, "")
}

type FailInput struct {
	OperationID, ClusterID, Message string
	ReleaseCapacity                 bool
}

// FailOperation is the end of a compensation path: the cluster is left FAILED (the tenant can
// delete it), capacity is released if nothing remains provisioned.
func (a *Activities) FailOperation(ctx context.Context, in FailInput) error {
	if in.ReleaseCapacity {
		if err := a.Store.ReleaseClusterReservations(ctx, in.ClusterID); err != nil {
			return err
		}
	}
	if err := a.Store.SetClusterState(ctx, in.ClusterID, model.ClusterFailed, truncate(in.Message, 300)); err != nil {
		return err
	}
	_ = a.Store.Audit(ctx, "workflow", "operation.failed", in.ClusterID, in.OperationID, map[string]any{"error": in.Message})
	return a.Store.SetOperationState(ctx, in.OperationID, model.OpFailed, truncate(in.Message, 1000))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
