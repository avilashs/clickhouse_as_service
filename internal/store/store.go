// Package store is the control plane's PostgreSQL access layer: the authoritative desired
// state. Every write used by a workflow activity is idempotent, so a retried activity (for
// example after the worker was killed) never creates duplicates.
package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

//go:embed migrations/*.sql
var migrations embed.FS

var (
	ErrNotFound            = errors.New("not found")
	ErrConflict            = errors.New("conflict")
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
	ErrCapacity            = errors.New("insufficient capacity")
)

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Migrate applies embedded migrations in order, once each.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, e := range entries {
		sql, err := migrations.ReadFile("migrations/" + e.Name())
		if err != nil {
			return err
		}
		err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			// serialise concurrent migrators (API and worker may start together)
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4242)`); err != nil {
				return err
			}
			var done bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)`, e.Name()).Scan(&done); err != nil {
				return err
			}
			if done {
				return nil
			}
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("%s: %w", e.Name(), err)
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, e.Name())
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func isUnique(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}

// ---------------------------------------------------------------- cells

// CellCapacity is the sellable capacity of one pool in a cell.
type CellCapacity struct {
	Pool        string
	MilliCPU    int64
	MemoryBytes int64
	Pods        int
}

// EnsureCell registers or updates a cell (idempotent).
func (s *Store) EnsureCell(ctx context.Context, c model.Cell, maxClusters int, agentTokenSHA256 string, caps []CellCapacity) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO cells (id, region, state, profile, domain, max_clusters, agent_token_sha256, sandbox_runtime, backup_target)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (id) DO UPDATE SET region=$2, profile=$4, domain=$5, max_clusters=$6, agent_token_sha256=$7, sandbox_runtime=$8, backup_target=$9`,
			c.ID, c.Region, c.State, c.Profile, c.Domain, maxClusters, agentTokenSHA256, c.Sandbox, backupJSON(c.Backup))
		if err != nil {
			return err
		}
		for _, cp := range caps {
			if _, err := tx.Exec(ctx, `
				INSERT INTO cell_capacity (cell_id, pool, milli_cpu_total, memory_total, pods_total) VALUES ($1,$2,$3,$4,$5)
				ON CONFLICT (cell_id, pool) DO UPDATE SET milli_cpu_total=$3, memory_total=$4, pods_total=$5`,
				c.ID, cp.Pool, cp.MilliCPU, cp.MemoryBytes, cp.Pods); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) GetCell(ctx context.Context, id string) (model.Cell, string, error) {
	var c model.Cell
	var tokenHash string
	var bt []byte
	err := s.pool.QueryRow(ctx, `SELECT id, region, state, profile, domain, ca_pem, agent_token_sha256, sandbox_runtime, backup_target FROM cells WHERE id=$1`, id).
		Scan(&c.ID, &c.Region, &c.State, &c.Profile, &c.Domain, &c.CAPEM, &tokenHash, &c.Sandbox, &bt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, "", ErrNotFound
	}
	if err == nil && len(bt) > 0 && string(bt) != "null" {
		c.Backup = &model.BackupTarget{}
		err = json.Unmarshal(bt, c.Backup)
	}
	return c, tokenHash, err
}

// backupJSON is the jsonb value of a cell's backup target (SQL NULL when there is none).
func backupJSON(b *model.BackupTarget) any {
	if b == nil {
		return nil
	}
	raw, _ := json.Marshal(b)
	return raw
}

// PickCell chooses an ACTIVE cell with room for one more cluster. M1 has one cell; this is
// where cell selection by region and fill level will live.
func (s *Store) PickCell(ctx context.Context) (model.Cell, error) {
	var id string
	err := s.pool.QueryRow(ctx, `
		SELECT c.id FROM cells c
		WHERE c.state = 'ACTIVE'
		  AND (SELECT count(*) FROM clusters k WHERE k.cell_id = c.id AND k.deleted_at IS NULL) < c.max_clusters
		ORDER BY c.id LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Cell{}, ErrCapacity
	}
	if err != nil {
		return model.Cell{}, err
	}
	c, _, err := s.GetCell(ctx, id)
	return c, err
}

func (s *Store) TouchAgent(ctx context.Context, cellID, caPEM string) error {
	_, err := s.pool.Exec(ctx, `UPDATE cells SET agent_last_seen_at = now(), ca_pem = CASE WHEN $2 = '' THEN ca_pem ELSE $2 END WHERE id = $1`, cellID, caPEM)
	return err
}

// ---------------------------------------------------------------- clusters and operations

// CreateClusterRequest carries everything the API persists atomically for a create.
type CreateClusterRequest struct {
	Cluster        model.Cluster
	Operation      model.Operation
	IdempotencyKey string
	RequestHash    string
	// Params are stored with the operation (restore: which backup to restore into the new cluster).
	Params map[string]string
}

// CreateCluster inserts cluster, spec v1, operation and idempotency key in one transaction.
// Replaying the same key with the same request hash returns the original operation.
func (s *Store) CreateCluster(ctx context.Context, r CreateClusterRequest) (model.Operation, bool, error) {
	var out model.Operation
	created := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		op, found, err := lookupIdempotency(ctx, tx, r.Cluster.ProjectID, r.IdempotencyKey, r.RequestHash)
		if err != nil || found {
			out = op
			return err
		}
		c := r.Cluster
		spec, _ := json.Marshal(c.Spec)
		if _, err := tx.Exec(ctx, `
			INSERT INTO clusters (id, project_id, cell_id, name, engine, state, desired_spec_version)
			VALUES ($1,$2,$3,$4,$5,$6,$7)`, c.ID, c.ProjectID, c.CellID, c.Name, c.Spec.Engine, c.State, c.Spec.Version); err != nil {
			if isUnique(err) {
				return fmt.Errorf("%w: a cluster named %q already exists", ErrConflict, c.Name)
			}
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO cluster_specs (cluster_id, version, spec) VALUES ($1,$2,$3)`, c.ID, c.Spec.Version, spec); err != nil {
			return err
		}
		if err := insertOperation(ctx, tx, r.Operation, r.IdempotencyKey, r.RequestHash); err != nil {
			return err
		}
		if len(r.Params) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE operations SET params=$2 WHERE id=$1`, r.Operation.ID, mustJSON(r.Params)); err != nil {
				return err
			}
		}
		out, created = r.Operation, true
		return nil
	})
	return out, created, err
}

// StartOperation records a new operation on an existing cluster (e.g. delete).
func (s *Store) StartOperation(ctx context.Context, op model.Operation, idemKey, reqHash string) (model.Operation, bool, error) {
	var out model.Operation
	created := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		existing, found, err := lookupIdempotency(ctx, tx, op.ProjectID, idemKey, reqHash)
		if err != nil || found {
			out = existing
			return err
		}
		var state string
		err = tx.QueryRow(ctx, `SELECT state FROM clusters WHERE id=$1 AND project_id=$2 AND deleted_at IS NULL FOR UPDATE`, op.ClusterID, op.ProjectID).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if err := insertOperation(ctx, tx, op, idemKey, reqHash); err != nil {
			return err
		}
		out, created = op, true
		return nil
	})
	return out, created, err
}

func lookupIdempotency(ctx context.Context, tx pgx.Tx, projectID, key, hash string) (model.Operation, bool, error) {
	var opID, storedHash string
	err := tx.QueryRow(ctx, `SELECT operation_id, request_hash FROM idempotency_keys WHERE project_id=$1 AND key=$2`, projectID, key).Scan(&opID, &storedHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Operation{}, false, nil
	}
	if err != nil {
		return model.Operation{}, false, err
	}
	if storedHash != hash {
		return model.Operation{}, false, ErrIdempotencyMismatch
	}
	op, err := getOperation(ctx, tx, opID)
	return op, true, err
}

func insertOperation(ctx context.Context, tx pgx.Tx, op model.Operation, key, hash string) error {
	if _, err := tx.Exec(ctx, `INSERT INTO operations (id, project_id, cluster_id, kind, state) VALUES ($1,$2,$3,$4,$5)`,
		op.ID, op.ProjectID, op.ClusterID, op.Kind, op.State); err != nil {
		if isUnique(err) {
			return fmt.Errorf("%w: another operation is in progress on cluster %s", ErrConflict, op.ClusterID)
		}
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (project_id, key, request_hash, operation_id) VALUES ($1,$2,$3,$4)`,
		op.ProjectID, key, hash, op.ID)
	return err
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func getOperation(ctx context.Context, q querier, id string) (model.Operation, error) {
	var op model.Operation
	err := q.QueryRow(ctx, `SELECT id, project_id, cluster_id, kind, state, error, created_at, updated_at FROM operations WHERE id=$1`, id).
		Scan(&op.ID, &op.ProjectID, &op.ClusterID, &op.Kind, &op.State, &op.Error, &op.CreatedAt, &op.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return op, ErrNotFound
	}
	return op, err
}

func (s *Store) GetOperation(ctx context.Context, id string) (model.Operation, error) {
	return getOperation(ctx, s.pool, id)
}

// SetOperationState is idempotent; terminal states are never overwritten.
func (s *Store) SetOperationState(ctx context.Context, id string, st model.OperationState, errMsg string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE operations SET state=$2, error=$3, updated_at=now()
		WHERE id=$1 AND state NOT IN ('SUCCEEDED','FAILED')`, id, st, errMsg)
	return err
}

// PendingOperations are operations whose workflow may never have been started (API crashed
// between commit and start). The worker's sweeper (re)starts them; start is idempotent.
func (s *Store) PendingOperations(ctx context.Context, olderThan time.Duration) ([]model.Operation, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM operations WHERE state='PENDING' AND created_at < now() - $1::interval`, fmt.Sprintf("%d seconds", int(olderThan.Seconds())))
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	var out []model.Operation
	for _, id := range ids {
		op, err := s.GetOperation(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, nil
}

const clusterCols = `c.id, c.project_id, c.cell_id, c.name, c.state, c.state_reason, s.spec, c.created_at, c.updated_at`

func scanCluster(row pgx.Row) (model.Cluster, error) {
	var c model.Cluster
	var spec []byte
	if err := row.Scan(&c.ID, &c.ProjectID, &c.CellID, &c.Name, &c.State, &c.StateReason, &spec, &c.CreatedAt, &c.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return c, ErrNotFound
		}
		return c, err
	}
	return c, json.Unmarshal(spec, &c.Spec)
}

// GetCluster returns the cluster with its desired spec. projectID "" skips the tenancy check
// (workflow use only).
func (s *Store) GetCluster(ctx context.Context, projectID, id string) (model.Cluster, error) {
	return scanCluster(s.pool.QueryRow(ctx, `
		SELECT `+clusterCols+` FROM clusters c JOIN cluster_specs s ON s.cluster_id=c.id AND s.version=c.desired_spec_version
		WHERE c.id=$1 AND ($2 = '' OR c.project_id=$2) AND c.deleted_at IS NULL`, id, projectID))
}

func (s *Store) ListClusters(ctx context.Context, projectID string) ([]model.Cluster, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+clusterCols+` FROM clusters c JOIN cluster_specs s ON s.cluster_id=c.id AND s.version=c.desired_spec_version
		WHERE c.project_id=$1 AND c.deleted_at IS NULL ORDER BY c.created_at`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Cluster
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) SetClusterState(ctx context.Context, id string, st model.ClusterState, reason string) error {
	_, err := s.pool.Exec(ctx, `UPDATE clusters SET state=$2, state_reason=$3, updated_at=now() WHERE id=$1 AND deleted_at IS NULL`, id, st, reason)
	return err
}

// ListServingClusters returns the clusters that are ACTIVE or DEGRADED: the ones whose health
// is derived from what the cell agent observes rather than owned by a workflow.
func (s *Store) ListServingClusters(ctx context.Context) ([]model.Cluster, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+clusterCols+` FROM clusters c JOIN cluster_specs s ON s.cluster_id=c.id AND s.version=c.desired_spec_version
		WHERE c.state IN ('ACTIVE','DEGRADED') AND c.deleted_at IS NULL ORDER BY c.created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Cluster
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// SetServingHealth moves a cluster between ACTIVE and DEGRADED, and only while it is one of the
// two: a concurrent delete or create-compensation has already moved it elsewhere and must win.
// It reports whether anything changed.
func (s *Store) SetServingHealth(ctx context.Context, id string, st model.ClusterState, reason string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE clusters SET state=$2, state_reason=$3, updated_at=now()
		WHERE id=$1 AND state IN ('ACTIVE','DEGRADED') AND deleted_at IS NULL AND (state<>$2 OR state_reason<>$3)`, id, st, reason)
	return tag.RowsAffected() > 0, err
}

// MarkClusterActive records that the desired spec has been reached.
func (s *Store) MarkClusterActive(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE clusters SET state='ACTIVE', state_reason='', current_spec_version=desired_spec_version, updated_at=now() WHERE id=$1 AND deleted_at IS NULL`, id)
	return err
}

// MarkClusterDeleted soft-deletes the cluster (the name becomes reusable).
func (s *Store) MarkClusterDeleted(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE clusters SET state='DELETED', state_reason='', deleted_at=COALESCE(deleted_at, now()), updated_at=now() WHERE id=$1`, id)
	return err
}

// ---------------------------------------------------------------- capacity

// Reserve holds capacity for an operation. It is idempotent per operation and serialised per
// pool with a row lock, so two concurrent creates cannot both take the last slot.
func (s *Store) Reserve(ctx context.Context, opID, cellID, clusterID string, p engine.PlacementPlan) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM capacity_reservations WHERE operation_id=$1)`, opID).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
		var cpuTotal, memTotal int64
		var podsTotal int
		err := tx.QueryRow(ctx, `SELECT milli_cpu_total, memory_total, pods_total FROM cell_capacity WHERE cell_id=$1 AND pool=$2 FOR UPDATE`, cellID, p.Pool).
			Scan(&cpuTotal, &memTotal, &podsTotal)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: cell %s has no %s pool", ErrCapacity, cellID, p.Pool)
		}
		if err != nil {
			return err
		}
		var cpuUsed, memUsed int64
		var podsUsed int
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(sum(milli_cpu),0), COALESCE(sum(memory),0), COALESCE(sum(pods),0)::int FROM capacity_reservations
			WHERE cell_id=$1 AND pool=$2 AND state IN ('HELD','COMMITTED')`, cellID, p.Pool).Scan(&cpuUsed, &memUsed, &podsUsed); err != nil {
			return err
		}
		if cpuUsed+p.MilliCPU > cpuTotal || memUsed+p.MemoryBytes > memTotal || podsUsed+p.Pods > podsTotal {
			return fmt.Errorf("%w: pool %s in cell %s", ErrCapacity, p.Pool, cellID)
		}
		_, err = tx.Exec(ctx, `INSERT INTO capacity_reservations (operation_id, cell_id, cluster_id, pool, milli_cpu, memory, pods, state)
			VALUES ($1,$2,$3,$4,$5,$6,$7,'HELD')`, opID, cellID, clusterID, p.Pool, p.MilliCPU, p.MemoryBytes, p.Pods)
		return err
	})
}

func (s *Store) CommitReservation(ctx context.Context, opID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE capacity_reservations SET state='COMMITTED', updated_at=now() WHERE operation_id=$1 AND state='HELD'`, opID)
	return err
}

func (s *Store) ReleaseClusterReservations(ctx context.Context, clusterID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE capacity_reservations SET state='RELEASED', updated_at=now() WHERE cluster_id=$1 AND state<>'RELEASED'`, clusterID)
	return err
}

// ---------------------------------------------------------------- desired / observed state

// PublishDesired stores the desired payload for a cluster and returns its generation. An
// identical payload (same hash, same tombstone flag) is a no-op returning the existing
// generation, which is what makes the publish activity safe to retry.
func (s *Store) PublishDesired(ctx context.Context, clusterID, cellID string, payload []byte, hash string, tombstone bool) (int64, error) {
	var gen int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var curHash string
		var curTomb bool
		err := tx.QueryRow(ctx, `SELECT generation, payload_hash, tombstone FROM desired_clusters WHERE cluster_id=$1 FOR UPDATE`, clusterID).Scan(&gen, &curHash, &curTomb)
		if err == nil && curHash == hash && curTomb == tombstone {
			return nil
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err := tx.QueryRow(ctx, `UPDATE cells SET desired_generation = desired_generation + 1 WHERE id=$1 RETURNING desired_generation`, cellID).Scan(&gen); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO desired_clusters (cluster_id, cell_id, generation, payload_hash, tombstone, payload) VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (cluster_id) DO UPDATE SET generation=$3, payload_hash=$4, tombstone=$5, payload=$6, updated_at=now()`,
			clusterID, cellID, gen, hash, tombstone, payload)
		return err
	})
	return gen, err
}

// DesiredRow is one cluster's desired state as served to the agent.
type DesiredRow struct {
	ClusterID  string          `json:"clusterId"`
	Generation int64           `json:"generation"`
	Hash       string          `json:"hash"`
	Tombstone  bool            `json:"tombstone"`
	Payload    json.RawMessage `json:"payload"`
}

// DesiredForCell returns every desired row of a cell (snapshot) and the cell generation.
func (s *Store) DesiredForCell(ctx context.Context, cellID string) ([]DesiredRow, int64, error) {
	var cellGen int64
	if err := s.pool.QueryRow(ctx, `SELECT desired_generation FROM cells WHERE id=$1`, cellID).Scan(&cellGen); err != nil {
		return nil, 0, err
	}
	rows, err := s.pool.Query(ctx, `SELECT cluster_id, generation, payload_hash, tombstone, payload FROM desired_clusters WHERE cell_id=$1 ORDER BY cluster_id`, cellID)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []DesiredRow
	for rows.Next() {
		var r DesiredRow
		if err := rows.Scan(&r.ClusterID, &r.Generation, &r.Hash, &r.Tombstone, &r.Payload); err != nil {
			return nil, 0, err
		}
		out = append(out, r)
	}
	return out, cellGen, rows.Err()
}

// GetDesired returns one cluster's desired row.
func (s *Store) GetDesired(ctx context.Context, clusterID string) (DesiredRow, error) {
	var r DesiredRow
	err := s.pool.QueryRow(ctx, `SELECT cluster_id, generation, payload_hash, tombstone, payload FROM desired_clusters WHERE cluster_id=$1`, clusterID).
		Scan(&r.ClusterID, &r.Generation, &r.Hash, &r.Tombstone, &r.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// ForgetDesired removes a cluster's desired row once its deletion has been observed.
func (s *Store) ForgetDesired(ctx context.Context, clusterID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM desired_clusters WHERE cluster_id=$1 AND tombstone`, clusterID)
	return err
}

// RecordObserved stores the agent's latest report per cluster. Reports for clusters this cell
// does not own are ignored.
func (s *Store) RecordObserved(ctx context.Context, cellID string, reports []engine.Observed) error {
	b := &pgx.Batch{}
	for _, r := range reports {
		j, _ := json.Marshal(r)
		b.Queue(`INSERT INTO observed_clusters (cluster_id, cell_id, report, observed_at)
			SELECT $1,$2,$3,$4 WHERE EXISTS (SELECT 1 FROM clusters WHERE id=$1 AND cell_id=$2)
			ON CONFLICT (cluster_id) DO UPDATE SET report=$3, observed_at=$4`, r.ClusterID, cellID, j, r.ObservedAt)
	}
	return s.pool.SendBatch(ctx, b).Close()
}

func (s *Store) GetObserved(ctx context.Context, clusterID string) (*engine.Observed, error) {
	var j []byte
	err := s.pool.QueryRow(ctx, `SELECT report FROM observed_clusters WHERE cluster_id=$1`, clusterID).Scan(&j)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var o engine.Observed
	return &o, json.Unmarshal(j, &o)
}

// Audit appends to the control-plane audit log.
func (s *Store) Audit(ctx context.Context, actor, action, resource, requestID string, details map[string]any) error {
	j, _ := json.Marshal(details)
	_, err := s.pool.Exec(ctx, `INSERT INTO audit_log (actor, action, resource, request_id, details) VALUES ($1,$2,$3,$4,$5)`,
		actor, action, resource, requestID, j)
	return err
}

// Counts is used by tests to assert "no duplicates".
func (s *Store) Count(ctx context.Context, table, where string, args ...any) (int, error) {
	if strings.ContainsAny(table, " ;") {
		return 0, fmt.Errorf("bad table")
	}
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+table+` WHERE `+where, args...).Scan(&n)
	return n, err
}
