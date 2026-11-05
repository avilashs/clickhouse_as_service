package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

// StartUpdate records an update of a live cluster atomically: a new spec version, the cluster
// moved to UPDATING, the operation and the idempotency key. fromVersion is the desired spec
// version the caller based the change on; if another change got in first the update is refused
// instead of silently overwriting it. Only an ACTIVE cluster can be updated.
func (s *Store) StartUpdate(ctx context.Context, projectID string, fromVersion int, next model.ClusterSpec, op model.Operation, key, hash string) (model.Operation, bool, error) {
	var out model.Operation
	created := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		existing, found, err := lookupIdempotency(ctx, tx, projectID, key, hash)
		if err != nil || found {
			out = existing
			return err
		}
		var state string
		var desired int
		err = tx.QueryRow(ctx, `SELECT state, desired_spec_version FROM clusters WHERE id=$1 AND project_id=$2 AND deleted_at IS NULL FOR UPDATE`,
			op.ClusterID, projectID).Scan(&state, &desired)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if state != string(model.ClusterActive) {
			return fmt.Errorf("%w: the cluster is %s; only an ACTIVE cluster can be updated", ErrConflict, state)
		}
		if desired != fromVersion {
			return fmt.Errorf("%w: the cluster changed while this request was prepared; retry", ErrConflict)
		}
		var maxV int
		if err := tx.QueryRow(ctx, `SELECT max(version) FROM cluster_specs WHERE cluster_id=$1`, op.ClusterID).Scan(&maxV); err != nil {
			return err
		}
		next.Version = maxV + 1
		spec, _ := json.Marshal(next)
		if _, err := tx.Exec(ctx, `INSERT INTO cluster_specs (cluster_id, version, spec) VALUES ($1,$2,$3)`, op.ClusterID, next.Version, spec); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE clusters SET desired_spec_version=$2, state='UPDATING', state_reason='', updated_at=now() WHERE id=$1`, op.ClusterID, next.Version); err != nil {
			return err
		}
		if err := insertOperation(ctx, tx, op, key, hash); err != nil {
			return err
		}
		out, created = op, true
		return nil
	})
	return out, created, err
}

// CurrentSpec returns the spec the cluster last reached (not the one being rolled out).
func (s *Store) CurrentSpec(ctx context.Context, clusterID string) (model.ClusterSpec, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `
		SELECT s.spec FROM clusters c JOIN cluster_specs s ON s.cluster_id=c.id AND s.version=c.current_spec_version
		WHERE c.id=$1 AND c.deleted_at IS NULL`, clusterID).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.ClusterSpec{}, ErrNotFound
	}
	if err != nil {
		return model.ClusterSpec{}, err
	}
	var spec model.ClusterSpec
	return spec, json.Unmarshal(raw, &spec)
}

// RevertToCurrentSpec points the cluster's desired spec back at the last one it reached (an
// update failed). It is idempotent.
func (s *Store) RevertToCurrentSpec(ctx context.Context, clusterID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE clusters SET desired_spec_version=current_spec_version, updated_at=now() WHERE id=$1 AND deleted_at IS NULL AND current_spec_version > 0`, clusterID)
	return err
}

// FinishUpdateRollback ends an update that was rolled back (or never started): the cluster is
// ACTIVE again on its previous spec. The reason lives in the operation's error.
func (s *Store) FinishUpdateRollback(ctx context.Context, clusterID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE clusters SET state='ACTIVE', state_reason='', updated_at=now() WHERE id=$1 AND deleted_at IS NULL AND state='UPDATING'`, clusterID)
	return err
}

// ReleaseReservation releases what one operation holds (idempotent).
func (s *Store) ReleaseReservation(ctx context.Context, opID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE capacity_reservations SET state='RELEASED', updated_at=now() WHERE operation_id=$1 AND state<>'RELEASED'`, opID)
	return err
}

// ReplaceClusterReservation makes plan the cluster's whole committed reservation, held under
// opID: everything the cluster held before is released and replaced in one transaction, so the
// pool never counts the old and the new plan together once an update has succeeded. Idempotent.
func (s *Store) ReplaceClusterReservation(ctx context.Context, opID, cellID, clusterID string, p engine.PlacementPlan) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE capacity_reservations SET state='RELEASED', updated_at=now() WHERE cluster_id=$1 AND operation_id<>$2 AND state<>'RELEASED'`, clusterID, opID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO capacity_reservations (operation_id, cell_id, cluster_id, pool, milli_cpu, memory, pods, state)
			VALUES ($1,$2,$3,$4,$5,$6,$7,'COMMITTED')
			ON CONFLICT (operation_id) DO UPDATE SET pool=$4, milli_cpu=$5, memory=$6, pods=$7, state='COMMITTED', updated_at=now()`,
			opID, cellID, clusterID, p.Pool, p.MilliCPU, p.MemoryBytes, p.Pods)
		return err
	})
}
