package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/avilashs/clickhouse_as_service/internal/backup"
	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
	"github.com/avilashs/clickhouse_as_service/internal/store"
)

// Timeouts for backup work, overridable in tests.
var (
	BackupTimeout  = 3 * time.Hour
	RestoreTimeout = 3 * time.Hour
	VerifyTimeout  = 6 * time.Hour
)

// Secret names holding the object store's long-lived key on a cell without STS (the local
// fixture). They are referenced from jobs as placeholders and resolved only when the job is
// served to the cell agent.
const (
	BackupAccessKeySecret = "backup/access-key"
	BackupSecretKeySecret = "backup/secret-key"
)

// BackupSupport is what backup, restore and verification activities need beyond the store.
type BackupSupport struct {
	Provider backup.Provider
	Objects  backup.Objects // nil: sizes are not recorded and retention cannot delete objects
	Temporal client.Client  // starts the restore operations that verification creates
	// VerifySize is the size of the scratch cluster that restore verification restores into.
	VerifySize string
}

// BackupPath is the object-store prefix of one backup. It starts with the project and cluster
// so that a credential scoped to one cluster's prefix cannot touch another's.
func BackupPath(projectID, clusterID, backupID string) string {
	return projectID + "/" + clusterID + "/" + backupID
}

func jobURL(t model.BackupTarget, path string) string {
	return strings.TrimSuffix(t.URL, "/") + "/" + strings.Trim(path, "/") + "/"
}

// ---------------------------------------------------------------- starting a backup (API and scheduler)

// StartBackupOperation records a backup and starts its workflow. kind INCREMENTAL without a base
// uses the newest completed backup of the cluster.
func StartBackupOperation(ctx context.Context, st *store.Store, tc client.Client, projectID, clusterID string, kind model.BackupKind, base string, scheduled bool, key, hash string) (model.Operation, model.Backup, bool, error) {
	c, err := st.GetCluster(ctx, projectID, clusterID)
	if err != nil {
		return model.Operation{}, model.Backup{}, false, err
	}
	cell, _, err := st.GetCell(ctx, c.CellID)
	if err != nil {
		return model.Operation{}, model.Backup{}, false, err
	}
	if cell.Backup == nil {
		return model.Operation{}, model.Backup{}, false, fmt.Errorf("%w: this cell has no backup storage configured", store.ErrConflict)
	}
	if kind == "" {
		kind = model.BackupFull
	}
	if kind == model.BackupIncremental && base == "" {
		bs, err := st.ClusterBackups(ctx, clusterID)
		if err != nil {
			return model.Operation{}, model.Backup{}, false, err
		}
		for _, b := range bs {
			if b.State == model.BackupCompleted {
				base = b.ID
				break
			}
		}
		if base == "" {
			return model.Operation{}, model.Backup{}, false, fmt.Errorf("%w: there is no completed backup to build an incremental backup on", store.ErrConflict)
		}
	}
	id := model.NewID("b")
	b := model.Backup{ID: id, ClusterID: clusterID, ProjectID: projectID, CellID: c.CellID, Kind: kind, BaseBackupID: base,
		State: model.BackupRunning, Path: BackupPath(projectID, clusterID, id), Scheduled: scheduled}
	op := model.Operation{ID: model.NewID("o"), ProjectID: projectID, ClusterID: clusterID, Kind: model.OpBackupCluster, State: model.OpPending, CreatedAt: time.Now().UTC()}
	op, created, err := st.StartBackup(ctx, b, op, key, hash)
	if err != nil {
		return op, b, false, err
	}
	if created && tc != nil {
		_ = Start(ctx, tc, op) // the sweeper starts it if this fails
	}
	return op, b, created, nil
}

// ---------------------------------------------------------------- activities

// jobSpec builds the job for the agent: credentials limited to the backup's own prefix (and its
// chain, which an incremental backup reads), or placeholders for the cell's static key.
func (a *Activities) jobSpec(ctx context.Context, action string, target model.BackupTarget, b model.Backup) (model.JobSpec, error) {
	spec := model.JobSpec{Action: action, URL: jobURL(target, b.Path)}
	scope := backup.Scope{}
	chain, err := a.chain(ctx, b)
	if err != nil {
		return spec, err
	}
	if action == "backup" {
		scope.WritePrefix = b.Path
		for _, c := range chain[1:] {
			scope.ReadPrefixes = append(scope.ReadPrefixes, c.Path)
		}
		if b.BaseBackupID != "" {
			spec.BaseURL = jobURL(target, chain[1].Path)
		}
	} else {
		for _, c := range chain {
			scope.ReadPrefixes = append(scope.ReadPrefixes, c.Path)
		}
	}
	if a.Backup == nil || a.Backup.Provider == nil {
		return spec, temporal.NewNonRetryableApplicationError("no credential provider is configured for backups", ErrTypeInvalid, nil)
	}
	cr, err := a.Backup.Provider.Mint(ctx, scope)
	if err != nil {
		return spec, err
	}
	if a.Backup.Provider.Scoped() {
		spec.AccessKeyID, spec.SecretAccessKey, spec.SessionToken = cr.AccessKey, cr.SecretKey, cr.SessionToken
	} else {
		spec.AccessKeyID, spec.SecretAccessKey = "{{secret:"+BackupAccessKeySecret+"}}", "{{secret:"+BackupSecretKeySecret+"}}"
	}
	return spec, nil
}

// chain returns the backup followed by its bases, nearest first.
func (a *Activities) chain(ctx context.Context, b model.Backup) ([]model.Backup, error) {
	out := []model.Backup{b}
	for cur := b; cur.BaseBackupID != ""; {
		next, err := a.Store.GetBackup(ctx, "", cur.BaseBackupID)
		if err != nil {
			return nil, fmt.Errorf("base backup %s: %w", cur.BaseBackupID, err)
		}
		out = append(out, next)
		cur = next
		if len(out) > 1000 {
			return nil, fmt.Errorf("backup chain of %s is implausibly long", b.ID)
		}
	}
	return out, nil
}

type JobInput struct {
	OperationID string
	BackupID    string
	Action      string // backup | restore
	ClusterID   string // the cluster the job runs against
}

// StartJob creates the job the cell agent runs. Idempotent: the job id is derived from the
// operation, so a retry finds the job it already made.
func (a *Activities) StartJob(ctx context.Context, in JobInput) (string, error) {
	b, err := a.Store.GetBackup(ctx, "", in.BackupID)
	if err != nil {
		return "", err
	}
	cl, err := a.Store.GetCluster(ctx, "", in.ClusterID)
	if err != nil {
		return "", err
	}
	cell, _, err := a.Store.GetCell(ctx, cl.CellID)
	if err != nil {
		return "", err
	}
	if cell.Backup == nil {
		return "", temporal.NewNonRetryableApplicationError("the cell has no backup storage configured", ErrTypeInvalid, nil)
	}
	spec, err := a.jobSpec(ctx, in.Action, *cell.Backup, b)
	if err != nil {
		return "", err
	}
	raw, _ := json.Marshal(spec)
	id := in.Action + "-" + in.OperationID
	return id, a.Store.InsertJob(ctx, store.Job{ID: id, ClusterID: in.ClusterID, CellID: cl.CellID, Kind: in.Action, Spec: raw})
}

// JobOutcome is what a finished job reported.
type JobOutcome struct {
	State   string
	Message string
	Result  model.JobResult
}

// WaitForJob polls until the agent reports the job finished. It heartbeats like WaitForPhase.
func (a *Activities) WaitForJob(ctx context.Context, jobID string) (JobOutcome, error) {
	interval := a.PollInterval
	if interval == 0 {
		interval = 3 * time.Second
	}
	for {
		j, err := a.Store.GetJob(ctx, jobID)
		if err != nil {
			return JobOutcome{}, err
		}
		activity.RecordHeartbeat(ctx, j.State)
		if j.State == "SUCCEEDED" || j.State == "FAILED" {
			out := JobOutcome{State: j.State, Message: j.Message}
			_ = json.Unmarshal(j.Result, &out.Result)
			return out, nil
		}
		select {
		case <-ctx.Done():
			return JobOutcome{}, temporal.NewApplicationError("the job did not finish: "+j.Message, ErrTypeTimeout)
		case <-time.After(interval):
		}
	}
}

type FinishBackupInput struct {
	OperationID, BackupID, ClusterID string
	JobID                            string
	Outcome                          JobOutcome
}

// FinishBackup records the result. A job that failed ends the backup FAILED and removes what it
// wrote.
func (a *Activities) FinishBackup(ctx context.Context, in FinishBackupInput) error {
	b, err := a.Store.GetBackup(ctx, "", in.BackupID)
	if err != nil {
		return err
	}
	if in.Outcome.State != "SUCCEEDED" {
		msg := truncate("backup failed: "+in.Outcome.Message, 1000)
		if a.Backup != nil && a.Backup.Objects != nil {
			_, _ = a.Backup.Objects.DeletePrefix(ctx, b.Path+"/") // best effort: partial objects
		}
		_ = a.Store.FinishBackup(ctx, b.ID, model.BackupFailed, 0, msg, nil)
		_ = a.Store.RetireJob(ctx, in.JobID)
		_ = a.Store.Audit(ctx, "workflow", "backup.failed", b.ClusterID, in.OperationID, map[string]any{"backup": b.ID, "error": in.Outcome.Message})
		return a.Store.SetOperationState(ctx, in.OperationID, model.OpFailed, msg)
	}
	var size int64
	if a.Backup != nil && a.Backup.Objects != nil {
		if size, err = a.Backup.Objects.SizeOf(ctx, b.Path+"/"); err != nil {
			return err
		}
	}
	fp, _ := json.Marshal(in.Outcome.Result)
	if err := a.Store.FinishBackup(ctx, b.ID, model.BackupCompleted, size, "", fp); err != nil {
		return err
	}
	_ = a.Store.RetireJob(ctx, in.JobID)
	_ = a.Store.Audit(ctx, "workflow", "backup.completed", b.ClusterID, in.OperationID, map[string]any{"backup": b.ID, "sizeBytes": size})
	return a.Store.SetOperationState(ctx, in.OperationID, model.OpSucceeded, "")
}

// BackupParams returns the backup an operation was started for.
func (a *Activities) BackupParams(ctx context.Context, opID string) (string, error) {
	p, err := a.Store.OperationParams(ctx, opID)
	if err != nil {
		return "", err
	}
	if p["backupId"] == "" {
		return "", temporal.NewNonRetryableApplicationError("operation "+opID+" names no backup", ErrTypeInvalid, nil)
	}
	return p["backupId"], nil
}

// FinishRestore ends a restore: the new cluster becomes ACTIVE. A failed restore job fails the
// operation (the workflow then compensates, tearing the new cluster down).
func (a *Activities) FinishRestore(ctx context.Context, in FinishBackupInput) error {
	_ = a.Store.RetireJob(ctx, in.JobID)
	if err := a.FinishCreate(ctx, FinishInput{in.OperationID, in.ClusterID}); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------- workflows

// BackupCluster: one native BACKUP of the cluster's databases, run by the cell agent against the
// cluster's own servers, writing to the cell's object store with credentials limited to this
// backup's prefix. The cluster keeps serving; no other operation can run on it meanwhile.
func BackupCluster(ctx workflow.Context, in Input) error {
	sctx := shortOpts(ctx)
	if err := workflow.ExecuteActivity(sctx, a.MarkOperationRunning, in.OperationID).Get(ctx, nil); err != nil {
		return err
	}
	var backupID string
	if err := workflow.ExecuteActivity(sctx, a.BackupParams, in.OperationID).Get(ctx, &backupID); err != nil {
		return err
	}
	fail := func(err error) error {
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		_ = workflow.ExecuteActivity(shortOpts(dctx), a.FinishBackup, FinishBackupInput{
			OperationID: in.OperationID, BackupID: backupID, ClusterID: in.ClusterID, Outcome: JobOutcome{State: "FAILED", Message: errMessage(err)}}).Get(dctx, nil)
		return err
	}
	var jobID string
	if err := workflow.ExecuteActivity(sctx, a.StartJob, JobInput{in.OperationID, backupID, "backup", in.ClusterID}).Get(ctx, &jobID); err != nil {
		return fail(err)
	}
	var out JobOutcome
	if err := workflow.ExecuteActivity(waitOpts(ctx, BackupTimeout), a.WaitForJob, jobID).Get(ctx, &out); err != nil {
		return fail(err)
	}
	return workflow.ExecuteActivity(sctx, a.FinishBackup, FinishBackupInput{in.OperationID, backupID, in.ClusterID, jobID, out}).Get(ctx, nil)
}

// RestoreCluster creates a new cluster exactly as CreateCluster does, then restores the backup
// named by the operation into it with a native RESTORE run by the cell agent. A restore never
// overwrites an existing cluster. If the restore fails the new cluster is torn down.
func RestoreCluster(ctx workflow.Context, in Input) error {
	sctx := shortOpts(ctx)
	if err := workflow.ExecuteActivity(sctx, a.MarkOperationRunning, in.OperationID).Get(ctx, nil); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(sctx, a.ReserveCapacity, ReserveInput{in.OperationID, in.ClusterID}).Get(ctx, nil); err != nil {
		_ = workflow.ExecuteActivity(sctx, a.FailOperation, FailInput{in.OperationID, in.ClusterID, errMessage(err), true}).Get(ctx, nil)
		return err
	}
	var backupID string
	err := workflow.ExecuteActivity(sctx, a.BackupParams, in.OperationID).Get(ctx, &backupID)
	var gen int64
	if err == nil {
		err = workflow.ExecuteActivity(sctx, a.EnsureClusterSecrets, in.ClusterID).Get(ctx, nil)
	}
	if err == nil {
		err = workflow.ExecuteActivity(sctx, a.PublishDesired, PublishInput{ClusterID: in.ClusterID}).Get(ctx, &gen)
	}
	if err == nil {
		var h engine.Health
		err = workflow.ExecuteActivity(waitOpts(ctx, ConvergeTimeout), a.WaitForPhase,
			WaitInput{ClusterID: in.ClusterID, Generation: gen, Want: engine.PhaseReady}).Get(ctx, &h)
	}
	var jobID string
	var out JobOutcome
	if err == nil {
		err = workflow.ExecuteActivity(sctx, a.StartJob, JobInput{in.OperationID, backupID, "restore", in.ClusterID}).Get(ctx, &jobID)
	}
	if err == nil {
		err = workflow.ExecuteActivity(waitOpts(ctx, RestoreTimeout), a.WaitForJob, jobID).Get(ctx, &out)
	}
	if err == nil && out.State != "SUCCEEDED" {
		err = temporal.NewNonRetryableApplicationError("restore failed: "+out.Message, ErrTypeInvalid, nil)
	}
	if err != nil {
		compensate(ctx, in, errMessage(err))
		return err
	}
	return workflow.ExecuteActivity(sctx, a.FinishRestore, FinishBackupInput{in.OperationID, backupID, in.ClusterID, jobID, out}).Get(ctx, nil)
}

// ---------------------------------------------------------------- restore verification

// VerifyInput identifies the backup to verify.
type VerifyInput struct {
	VerificationID string
	BackupID       string
	Scheduled      bool
}

// VerifyBackup proves a backup restores: it restores it into a scratch cluster in a project no
// tenant can see, runs CHECK TABLE on every restored table, compares row counts and content
// hashes with the source's fingerprints taken around the backup, and deletes the scratch cluster
// whatever the outcome. A backup is "verified" only when this passes.
func VerifyBackup(ctx workflow.Context, in VerifyInput) error {
	sctx := shortOpts(ctx)
	var vs VerificationStart
	if err := workflow.ExecuteActivity(sctx, a.StartVerification, in).Get(ctx, &vs); err != nil {
		return err
	}
	dctx, _ := workflow.NewDisconnectedContext(ctx)
	cleanup := func() {
		var del string
		if err := workflow.ExecuteActivity(shortOpts(dctx), a.DeleteScratch, vs.ClusterID).Get(dctx, &del); err == nil && del != "" {
			_ = workflow.ExecuteActivity(waitOpts(dctx, DeleteTimeout+5*time.Minute), a.WaitForOperation, del).Get(dctx, nil)
		}
	}
	var fin string
	err := workflow.ExecuteActivity(waitOpts(ctx, VerifyTimeout), a.WaitForOperation, vs.OperationID).Get(ctx, &fin)
	if err == nil && fin != string(model.OpSucceeded) {
		err = fmt.Errorf("the restore into the scratch cluster %s", strings.ToLower(fin))
	}
	var detail VerifyDetail
	if err == nil {
		err = workflow.ExecuteActivity(sctx, a.CompareRestore, vs).Get(ctx, &detail)
	}
	cleanup()
	if err != nil {
		_ = workflow.ExecuteActivity(shortOpts(dctx), a.FinishVerification, FinishVerificationInput{vs.VerificationID, vs.BackupID, false, VerifyDetail{Problems: []string{errMessage(err)}}}).Get(dctx, nil)
		return err
	}
	return workflow.ExecuteActivity(shortOpts(dctx), a.FinishVerification, FinishVerificationInput{vs.VerificationID, vs.BackupID, len(detail.Problems) == 0, detail}).Get(dctx, nil)
}

type VerificationStart struct {
	VerificationID, BackupID, ClusterID, OperationID, JobID string
}

// StartVerification creates the scratch cluster (a restore operation in the scratch project) and
// starts it.
func (a *Activities) StartVerification(ctx context.Context, in VerifyInput) (VerificationStart, error) {
	vid := in.VerificationID
	var out VerificationStart
	b, err := a.Store.GetBackup(ctx, "", in.BackupID)
	if err != nil {
		return out, err
	}
	if b.State != model.BackupCompleted {
		return out, temporal.NewNonRetryableApplicationError("only a completed backup can be verified", ErrTypeInvalid, nil)
	}
	spec, err := a.Store.LastSpec(ctx, b.ClusterID)
	if err != nil {
		return out, err
	}
	size := a.Backup.VerifySize
	if size == "" {
		size = "dev-1"
	}
	scratch := spec
	scratch.Version, scratch.Size, scratch.Shards = 1, size, 1
	scratch.ReplicasPerShard = model.Sizes[size].Replicas
	scratch.CompatibilityPin = ""
	// ids derived from the verification's, so that a retried activity finds what it made
	cid, oid := "c"+strings.TrimPrefix(vid, "v"), "o"+strings.TrimPrefix(vid, "v")
	cell, _, err := a.Store.GetCell(ctx, b.CellID)
	if err != nil {
		return out, err
	}
	c := model.Cluster{ID: cid, ProjectID: model.ScratchProject, CellID: b.CellID, Name: "verify-" + b.ID, State: model.ClusterCreating, Spec: scratch, CreatedAt: time.Now().UTC()}
	if errs := a.Engines["clickhouse"].Validate(scratch, cell); len(errs) > 0 {
		return out, temporal.NewNonRetryableApplicationError(fmt.Sprintf("scratch cluster spec is invalid: %v", errs), ErrTypeInvalid, nil)
	}
	op := model.Operation{ID: oid, ProjectID: model.ScratchProject, ClusterID: cid, Kind: model.OpRestoreCluster, State: model.OpPending, CreatedAt: time.Now().UTC()}
	op, _, err = a.Store.CreateCluster(ctx, store.CreateClusterRequest{Cluster: c, Operation: op, IdempotencyKey: "verify-" + vid, RequestHash: vid,
		Params: map[string]string{"backupId": b.ID}})
	if err != nil {
		return out, err
	}
	if err := a.Store.StartVerification(ctx, store.Verification{ID: vid, BackupID: b.ID, ScratchClusterID: op.ClusterID, Scheduled: in.Scheduled}); err != nil {
		return out, err
	}
	if a.Backup != nil && a.Backup.Temporal != nil {
		if err := Start(ctx, a.Backup.Temporal, op); err != nil {
			return out, err
		}
	}
	return VerificationStart{vid, b.ID, op.ClusterID, op.ID, "restore-" + op.ID}, nil
}

// WaitForOperation polls until an operation is finished and returns its state.
func (a *Activities) WaitForOperation(ctx context.Context, opID string) (string, error) {
	interval := a.PollInterval
	if interval == 0 {
		interval = 5 * time.Second
	}
	for {
		op, err := a.Store.GetOperation(ctx, opID)
		if err != nil {
			return "", err
		}
		activity.RecordHeartbeat(ctx, op.State)
		if op.State == model.OpSucceeded || op.State == model.OpFailed {
			return string(op.State), nil
		}
		select {
		case <-ctx.Done():
			return "", temporal.NewApplicationError("operation "+opID+" did not finish", ErrTypeTimeout)
		case <-time.After(interval):
		}
	}
}

// DeleteScratch starts the deletion of the scratch cluster and returns the operation to wait for.
// A scratch cluster that is already gone returns "".
func (a *Activities) DeleteScratch(ctx context.Context, clusterID string) (string, error) {
	c, err := a.Store.GetCluster(ctx, model.ScratchProject, clusterID)
	if err != nil {
		return "", nil // gone
	}
	op := model.Operation{ID: model.NewID("o"), ProjectID: model.ScratchProject, ClusterID: c.ID, Kind: model.OpDeleteCluster, State: model.OpPending, CreatedAt: time.Now().UTC()}
	// a restore that failed is compensating itself; wait for that to finish before deleting
	op, _, err = a.Store.StartOperation(ctx, op, "verify-del-"+clusterID, "del")
	if err != nil {
		return "", err
	}
	if a.Backup != nil && a.Backup.Temporal != nil {
		if err := Start(ctx, a.Backup.Temporal, op); err != nil {
			return "", err
		}
	}
	return op.ID, nil
}

// VerifyDetail is the outcome of comparing a restore with the backup's fingerprints.
type VerifyDetail struct {
	Tables   int      `json:"tables"`
	Exact    int      `json:"exact"`    // tables whose rows and content hash equal the source's
	Bounded  int      `json:"bounded"`  // tables written to during the backup: rows within the bounds
	Problems []string `json:"problems"` // empty when the backup verified
}

// CompareRestore compares what the scratch cluster holds with what the source held around the
// backup. A table nobody wrote to during the backup must match exactly, rows and content hash. A
// table that was being written to can only be checked for CHECK TABLE and for a row count
// between its count before and after the backup (inserts only add).
func (a *Activities) CompareRestore(ctx context.Context, vs VerificationStart) (VerifyDetail, error) {
	b, err := a.Store.GetBackup(ctx, "", vs.BackupID)
	if err != nil {
		return VerifyDetail{}, err
	}
	j, err := a.Store.GetJob(ctx, vs.JobID)
	if err != nil {
		return VerifyDetail{}, err
	}
	var src, got model.JobResult
	if err := json.Unmarshal(b.Fingerprint, &src); err != nil {
		return VerifyDetail{}, err
	}
	if err := json.Unmarshal(j.Result, &got); err != nil {
		return VerifyDetail{}, err
	}
	_ = a.Store.RetireJob(ctx, j.ID)
	return CompareFingerprints(src, got), nil
}

// CompareFingerprints is the verification rule (unit tested).
func CompareFingerprints(src, got model.JobResult) VerifyDetail {
	d := VerifyDetail{Problems: []string{}}
	if len(src.Before) == 0 {
		d.Problems = append(d.Problems, "the backup recorded no tables (nothing to verify against)")
	}
	for t, before := range src.Before {
		d.Tables++
		after := src.After[t]
		r, ok := got.Restored[t]
		if !ok {
			d.Problems = append(d.Problems, t+": missing from the restored cluster")
			continue
		}
		if msg := got.Checks[t]; msg != "" {
			d.Problems = append(d.Problems, t+": CHECK TABLE: "+msg)
		}
		switch {
		case before == after:
			if r != before {
				d.Problems = append(d.Problems, fmt.Sprintf("%s: restored %d rows (hash %s), the source held %d rows (hash %s)", t, r.Rows, r.Hash, before.Rows, before.Hash))
			} else {
				d.Exact++
			}
		default:
			lo, hi := before.Rows, after.Rows
			if lo > hi {
				lo, hi = hi, lo
			}
			if r.Rows < lo || r.Rows > hi {
				d.Problems = append(d.Problems, fmt.Sprintf("%s: restored %d rows, outside what the source held during the backup (%d..%d)", t, r.Rows, lo, hi))
			} else {
				d.Bounded++
			}
		}
	}
	for t := range got.Restored {
		if _, ok := src.Before[t]; !ok {
			d.Problems = append(d.Problems, t+": restored but not in the backup's record")
		}
	}
	return d
}

type FinishVerificationInput struct {
	VerificationID, BackupID string
	Passed                   bool
	Detail                   VerifyDetail
}

func (a *Activities) FinishVerification(ctx context.Context, in FinishVerificationInput) error {
	raw, _ := json.Marshal(in.Detail)
	if err := a.Store.FinishVerification(ctx, in.VerificationID, in.Passed, raw); err != nil {
		return err
	}
	if in.Passed {
		if err := a.Store.MarkBackupVerified(ctx, in.BackupID); err != nil {
			return err
		}
	}
	_ = a.Store.Audit(ctx, "workflow", "backup.verified", in.BackupID, in.VerificationID, map[string]any{"passed": in.Passed, "problems": in.Detail.Problems})
	return nil
}

// StartVerificationWorkflow starts a restore verification of a backup and returns its id.
func StartVerificationWorkflow(ctx context.Context, c client.Client, backupID string, scheduled bool) (string, error) {
	vid := model.NewID("v")
	_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "verify/" + vid, TaskQueue: TaskQueue},
		VerifyBackup, VerifyInput{VerificationID: vid, BackupID: backupID, Scheduled: scheduled})
	return vid, err
}
