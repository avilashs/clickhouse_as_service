package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/avilashs/clickhouse_as_service/internal/model"
)

const backupCols = `id, cluster_id, project_id, cell_id, kind, COALESCE(base_backup_id,''), state, path, scheduled, size_bytes, error, fingerprint, created_at, finished_at, verified_at`

func scanBackup(row pgx.Row) (model.Backup, error) {
	var b model.Backup
	err := row.Scan(&b.ID, &b.ClusterID, &b.ProjectID, &b.CellID, &b.Kind, &b.BaseBackupID, &b.State, &b.Path, &b.Scheduled, &b.SizeBytes, &b.Error, &b.Fingerprint, &b.CreatedAt, &b.FinishedAt, &b.VerifiedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, ErrNotFound
	}
	return b, err
}

// StartBackup records a backup and its operation atomically. Only an ACTIVE cluster can be backed
// up, and, as with every operation, only one operation runs on a cluster at a time.
func (s *Store) StartBackup(ctx context.Context, b model.Backup, op model.Operation, key, hash string) (model.Operation, bool, error) {
	var out model.Operation
	created := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		existing, found, err := lookupIdempotency(ctx, tx, b.ProjectID, key, hash)
		if err != nil || found {
			out = existing
			return err
		}
		var state string
		err = tx.QueryRow(ctx, `SELECT state FROM clusters WHERE id=$1 AND project_id=$2 AND deleted_at IS NULL FOR UPDATE`, b.ClusterID, b.ProjectID).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if state != string(model.ClusterActive) {
			return fmt.Errorf("%w: the cluster is %s; only an ACTIVE cluster can be backed up", ErrConflict, state)
		}
		if b.Kind == model.BackupIncremental {
			// the base must be a completed backup of this cluster, and it must not be going away
			var baseState string
			if err := tx.QueryRow(ctx, `SELECT state FROM backups WHERE id=$1 AND cluster_id=$2`, b.BaseBackupID, b.ClusterID).Scan(&baseState); err != nil || baseState != string(model.BackupCompleted) {
				return fmt.Errorf("%w: the base backup %s is not a completed backup of this cluster", ErrConflict, b.BaseBackupID)
			}
		}
		if err := insertOperation(ctx, tx, op, key, hash); err != nil {
			return err
		}
		var base any
		if b.BaseBackupID != "" {
			base = b.BaseBackupID
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO backups (id, cluster_id, project_id, cell_id, kind, base_backup_id, state, path, scheduled)
			VALUES ($1,$2,$3,$4,$5,$6,'RUNNING',$7,$8)`, b.ID, b.ClusterID, b.ProjectID, b.CellID, b.Kind, base, b.Path, b.Scheduled); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE operations SET params=$2 WHERE id=$1`, op.ID, mustJSON(map[string]string{"backupId": b.ID})); err != nil {
			return err
		}
		out, created = op, true
		return nil
	})
	return out, created, err
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// OperationParams returns the parameters stored with an operation.
func (s *Store) OperationParams(ctx context.Context, opID string) (map[string]string, error) {
	var raw []byte
	if err := s.pool.QueryRow(ctx, `SELECT params FROM operations WHERE id=$1`, opID).Scan(&raw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	out := map[string]string{}
	return out, json.Unmarshal(raw, &out)
}

func (s *Store) SetOperationParams(ctx context.Context, opID string, params map[string]string) error {
	_, err := s.pool.Exec(ctx, `UPDATE operations SET params=$2 WHERE id=$1`, opID, mustJSON(params))
	return err
}

func (s *Store) GetBackup(ctx context.Context, projectID, id string) (model.Backup, error) {
	return scanBackup(s.pool.QueryRow(ctx, `SELECT `+backupCols+` FROM backups WHERE id=$1 AND ($2='' OR project_id=$2) AND state<>'DELETED'`, id, projectID))
}

func (s *Store) ListBackups(ctx context.Context, projectID, clusterID string) ([]model.Backup, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+backupCols+` FROM backups WHERE project_id=$1 AND cluster_id=$2 AND state<>'DELETED' ORDER BY created_at DESC`, projectID, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Backup
	for rows.Next() {
		b, err := scanBackup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ClusterBackups lists every non-deleted backup of a cluster, newest first, whoever owns it
// (scheduling and retention).
func (s *Store) ClusterBackups(ctx context.Context, clusterID string) ([]model.Backup, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+backupCols+` FROM backups WHERE cluster_id=$1 AND state<>'DELETED' ORDER BY created_at DESC`, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Backup
	for rows.Next() {
		b, err := scanBackup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// FinishBackup records how a backup ended. It never moves a backup out of a terminal state.
func (s *Store) FinishBackup(ctx context.Context, id string, state model.BackupState, size int64, errMsg string, fingerprint []byte) error {
	if len(fingerprint) == 0 {
		fingerprint = []byte("{}")
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE backups SET state=$2, size_bytes=$3, error=$4, fingerprint=$5, finished_at=now()
		WHERE id=$1 AND state='RUNNING'`, id, state, size, errMsg, fingerprint)
	return err
}

func (s *Store) MarkBackupState(ctx context.Context, id string, state model.BackupState) error {
	_, err := s.pool.Exec(ctx, `UPDATE backups SET state=$2 WHERE id=$1`, id, state)
	return err
}

func (s *Store) MarkBackupVerified(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE backups SET verified_at=now() WHERE id=$1`, id)
	return err
}

// Dependants returns the live backups that name id as their base.
func (s *Store) Dependants(ctx context.Context, id string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM backups WHERE base_backup_id=$1 AND state NOT IN ('DELETED')`, id)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// ---------------------------------------------------------------- restore verification

type Verification struct {
	ID, BackupID, ScratchClusterID, State string
	Detail                                []byte
	Scheduled                             bool
	CreatedAt                             time.Time
	FinishedAt                            *time.Time
}

func (s *Store) StartVerification(ctx context.Context, v Verification) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO restore_verifications (id, backup_id, scratch_cluster_id, state, scheduled) VALUES ($1,$2,$3,'RUNNING',$4) ON CONFLICT (id) DO NOTHING`,
		v.ID, v.BackupID, v.ScratchClusterID, v.Scheduled)
	return err
}

func (s *Store) FinishVerification(ctx context.Context, id string, passed bool, detail []byte) error {
	state := "FAILED"
	if passed {
		state = "PASSED"
	}
	if len(detail) == 0 {
		detail = []byte("{}")
	}
	_, err := s.pool.Exec(ctx, `UPDATE restore_verifications SET state=$2, detail=$3, finished_at=now() WHERE id=$1 AND state='RUNNING'`, id, state, detail)
	return err
}

func (s *Store) Verifications(ctx context.Context, backupID string) ([]Verification, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, backup_id, scratch_cluster_id, state, detail, scheduled, created_at, finished_at FROM restore_verifications WHERE backup_id=$1 ORDER BY created_at DESC`, backupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Verification
	for rows.Next() {
		var v Verification
		if err := rows.Scan(&v.ID, &v.BackupID, &v.ScratchClusterID, &v.State, &v.Detail, &v.Scheduled, &v.CreatedAt, &v.FinishedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// LastVerification returns the newest verification (any state) of any backup of a cluster.
func (s *Store) LastVerification(ctx context.Context, clusterID string) (*Verification, error) {
	var v Verification
	err := s.pool.QueryRow(ctx, `
		SELECT r.id, r.backup_id, r.scratch_cluster_id, r.state, r.detail, r.scheduled, r.created_at, r.finished_at
		FROM restore_verifications r JOIN backups b ON b.id = r.backup_id
		WHERE b.cluster_id=$1 ORDER BY r.created_at DESC LIMIT 1`, clusterID).
		Scan(&v.ID, &v.BackupID, &v.ScratchClusterID, &v.State, &v.Detail, &v.Scheduled, &v.CreatedAt, &v.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &v, err
}

// ---------------------------------------------------------------- agent jobs

type Job struct {
	ID, ClusterID, CellID, Kind string
	Spec                        json.RawMessage
	State, Message              string
	Result                      json.RawMessage
}

// InsertJob creates a job for the agent to run. Idempotent per id.
func (s *Store) InsertJob(ctx context.Context, j Job) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO cluster_jobs (id, cluster_id, cell_id, kind, spec, state) VALUES ($1,$2,$3,$4,$5,'PENDING') ON CONFLICT (id) DO NOTHING`,
		j.ID, j.ClusterID, j.CellID, j.Kind, []byte(j.Spec))
	return err
}

func (s *Store) GetJob(ctx context.Context, id string) (Job, error) {
	var j Job
	err := s.pool.QueryRow(ctx, `SELECT id, cluster_id, cell_id, kind, spec, state, message, result FROM cluster_jobs WHERE id=$1`, id).
		Scan(&j.ID, &j.ClusterID, &j.CellID, &j.Kind, &j.Spec, &j.State, &j.Message, &j.Result)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, ErrNotFound
	}
	return j, err
}

// JobsForCell returns the jobs the agent should be running or reporting.
func (s *Store) JobsForCell(ctx context.Context, cellID string) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, cluster_id, cell_id, kind, spec, state, message, result FROM cluster_jobs WHERE cell_id=$1 AND served ORDER BY created_at`, cellID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.ClusterID, &j.CellID, &j.Kind, &j.Spec, &j.State, &j.Message, &j.Result); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// RecordJobStatus stores what the agent of a cell reported about one of that cell's jobs. A
// finished job is never overwritten.
func (s *Store) RecordJobStatus(ctx context.Context, cellID, id, state, message string, result json.RawMessage) error {
	if len(result) == 0 {
		result = json.RawMessage("{}")
	}
	_, err := s.pool.Exec(ctx, `UPDATE cluster_jobs SET state=$2, message=$3, result=$4, updated_at=now() WHERE id=$1 AND cell_id=$5 AND state NOT IN ('SUCCEEDED','FAILED')`,
		id, state, message, []byte(result), cellID)
	return err
}

// RetireJob stops serving a job (its result has been collected).
func (s *Store) RetireJob(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE cluster_jobs SET served=false, updated_at=now() WHERE id=$1`, id)
	return err
}

// LastSpec returns the newest spec a cluster was given, deleted or not (restore needs it for a
// backup whose cluster has since been deleted).
func (s *Store) LastSpec(ctx context.Context, clusterID string) (model.ClusterSpec, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT spec FROM cluster_specs WHERE cluster_id=$1 ORDER BY version DESC LIMIT 1`, clusterID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ClusterSpec{}, ErrNotFound
	}
	if err != nil {
		return model.ClusterSpec{}, err
	}
	var spec model.ClusterSpec
	return spec, json.Unmarshal(raw, &spec)
}

// BeginDeleteBackup marks a backup DELETING if nothing depends on it: no live backup built on it
// and no restore in flight reading it. The objects are removed by the retention loop.
func (s *Store) BeginDeleteBackup(ctx context.Context, projectID, id string) (model.Backup, error) {
	var out model.Backup
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		b, err := scanBackup(tx.QueryRow(ctx, `SELECT `+backupCols+` FROM backups WHERE id=$1 AND project_id=$2 AND state<>'DELETED' FOR UPDATE`, id, projectID))
		if err != nil {
			return err
		}
		if b.State == model.BackupRunning {
			return fmt.Errorf("%w: the backup is still running", ErrConflict)
		}
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM backups WHERE base_backup_id=$1 AND state<>'DELETED'`, id).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("%w: %d later backup(s) are built on this one; delete them first", ErrConflict, n)
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM operations WHERE kind='RESTORE_CLUSTER' AND state IN ('PENDING','RUNNING') AND params->>'backupId'=$1`, id).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("%w: a restore from this backup is in progress", ErrConflict)
		}
		if _, err := tx.Exec(ctx, `UPDATE backups SET state='DELETING' WHERE id=$1`, id); err != nil {
			return err
		}
		b.State = model.BackupDeleting
		out = b
		return nil
	})
	return out, err
}

// BackupsToExpire returns completed or failed backups created before the cutoff that nothing is
// built on, except the newest completed backup of each live cluster, which is always kept (a
// deleted cluster keeps none: its backups age out after the retention period). Run
// repeatedly, this unravels a chain from its newest member: a chain goes whole, never in part.
func (s *Store) BackupsToExpire(ctx context.Context, before time.Time) ([]model.Backup, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+backupCols+` FROM backups b
		WHERE b.state IN ('COMPLETED','FAILED') AND b.created_at < $1
		  AND NOT EXISTS (SELECT 1 FROM backups d WHERE d.base_backup_id=b.id AND d.state<>'DELETED')
		  AND NOT EXISTS (SELECT 1 FROM operations o WHERE o.kind='RESTORE_CLUSTER' AND o.state IN ('PENDING','RUNNING') AND o.params->>'backupId'=b.id)
		  AND (b.state='FAILED'
		       OR EXISTS (SELECT 1 FROM clusters c WHERE c.id=b.cluster_id AND c.deleted_at IS NOT NULL)
		       OR EXISTS (SELECT 1 FROM backups n WHERE n.cluster_id=b.cluster_id AND n.state='COMPLETED' AND n.created_at > b.created_at))
		ORDER BY b.created_at LIMIT 50`, before)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Backup
	for rows.Next() {
		b, err := scanBackup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// DeletingBackups returns the backups whose objects are to be removed.
func (s *Store) DeletingBackups(ctx context.Context) ([]model.Backup, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+backupCols+` FROM backups WHERE state='DELETING' ORDER BY created_at LIMIT 50`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Backup
	for rows.Next() {
		b, err := scanBackup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
