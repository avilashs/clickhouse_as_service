package workflows

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/avilashs/clickhouse_as_service/internal/backup"
	"github.com/avilashs/clickhouse_as_service/internal/model"
	"github.com/avilashs/clickhouse_as_service/internal/store"
)

// Schedule is the backup policy of a cell, from BASALT_BACKUP_* (internal/config).
type Schedule struct {
	Interval       time.Duration // a backup per cluster this often (0: no scheduled backups)
	FullEvery      int           // every Nth scheduled backup is a full one; the others are incremental
	Retention      time.Duration
	VerifyInterval time.Duration // a restore verification per cluster this often (0: none)
	Tick           time.Duration // how often the scheduler looks (default 30 s)
}

// RunScheduler starts scheduled backups and restore verifications, and expires old backups. It
// holds no state of its own: every decision is made from the backups table, so a restarted worker
// carries on, and a second worker would at worst find the one-operation-per-cluster lock taken.
func RunScheduler(ctx context.Context, log *slog.Logger, st *store.Store, tc client.Client, objs backup.Objects, sc Schedule) {
	tick := sc.Tick
	if tick == 0 {
		tick = 30 * time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if sc.Interval > 0 {
			if err := scheduleBackups(ctx, log, st, tc, sc); err != nil {
				log.Warn("backup scheduler", "err", err)
			}
		}
		if sc.VerifyInterval > 0 {
			if err := scheduleVerifications(ctx, log, st, tc, sc); err != nil {
				log.Warn("verification scheduler", "err", err)
			}
		}
		if objs != nil {
			if err := Retain(ctx, log, st, objs, sc.Retention, time.Now()); err != nil {
				log.Warn("backup retention", "err", err)
			}
		}
	}
}

// NextBackupKind decides whether the next scheduled backup is full or incremental: incremental
// on top of the newest completed backup, until FullEvery backups have been taken since the last
// full one.
func NextBackupKind(existing []model.Backup, fullEvery int) (model.BackupKind, string) {
	if fullEvery <= 1 {
		return model.BackupFull, ""
	}
	var newest *model.Backup
	for i := range existing { // newest first
		if existing[i].State == model.BackupCompleted {
			newest = &existing[i]
			break
		}
	}
	if newest == nil {
		return model.BackupFull, ""
	}
	byID := map[string]model.Backup{}
	for _, b := range existing {
		byID[b.ID] = b
	}
	length := 1
	for cur := *newest; cur.BaseBackupID != ""; length++ {
		next, ok := byID[cur.BaseBackupID]
		if !ok || length > 10000 {
			return model.BackupFull, ""
		}
		cur = next
	}
	if length >= fullEvery {
		return model.BackupFull, ""
	}
	return model.BackupIncremental, newest.ID
}

func scheduleBackups(ctx context.Context, log *slog.Logger, st *store.Store, tc client.Client, sc Schedule) error {
	clusters, err := st.ListServingClusters(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, c := range clusters {
		if c.ProjectID == model.ScratchProject || c.State != model.ClusterActive {
			continue
		}
		cell, _, err := st.GetCell(ctx, c.CellID)
		if err != nil || cell.Backup == nil {
			continue
		}
		existing, err := st.ClusterBackups(ctx, c.ID)
		if err != nil {
			return err
		}
		running := false
		var last time.Time
		for _, b := range existing {
			if b.State == model.BackupRunning {
				running = true
			}
			if b.CreatedAt.After(last) {
				last = b.CreatedAt
			}
		}
		if running || now.Sub(last) < sc.Interval {
			continue
		}
		kind, base := NextBackupKind(existing, sc.FullEvery)
		key := fmt.Sprintf("sched-backup-%s-%d", c.ID, now.Unix()/int64(sc.Interval.Seconds()+1))
		_, b, created, err := StartBackupOperation(ctx, st, tc, c.ProjectID, c.ID, kind, base, true, key, "scheduled")
		switch {
		case errors.Is(err, store.ErrConflict):
			log.Debug("scheduled backup waits", "cluster", c.ID, "reason", err)
		case err != nil:
			log.Warn("scheduled backup", "cluster", c.ID, "err", err)
		case created:
			log.Info("scheduled backup started", "cluster", c.ID, "backup", b.ID, "kind", b.Kind)
		}
	}
	return nil
}

func scheduleVerifications(ctx context.Context, log *slog.Logger, st *store.Store, tc client.Client, sc Schedule) error {
	running, err := st.Count(ctx, "restore_verifications", "state='RUNNING'")
	if err != nil {
		return err
	}
	if running > 0 {
		return nil // one scratch cluster at a time
	}
	clusters, err := st.ListServingClusters(ctx)
	if err != nil {
		return err
	}
	for _, c := range clusters {
		if c.ProjectID == model.ScratchProject {
			continue
		}
		bs, err := st.ClusterBackups(ctx, c.ID)
		if err != nil {
			return err
		}
		var newest *model.Backup
		for i := range bs {
			if bs[i].State == model.BackupCompleted {
				newest = &bs[i]
				break
			}
		}
		if newest == nil {
			continue
		}
		lv, err := st.LastVerification(ctx, c.ID)
		if err != nil {
			return err
		}
		if lv != nil && time.Since(lv.CreatedAt) < sc.VerifyInterval {
			continue
		}
		vid, err := StartVerificationWorkflow(ctx, tc, newest.ID, true)
		if err != nil {
			return err
		}
		log.Info("scheduled restore verification started", "cluster", c.ID, "backup", newest.ID, "verification", vid)
		return nil // one per pass
	}
	return nil
}

// Retain removes the objects of backups being deleted, and marks backups past retention for
// deletion when nothing is built on them.
func Retain(ctx context.Context, log *slog.Logger, st *store.Store, objs backup.Objects, retention time.Duration, now time.Time) error {
	if retention > 0 {
		old, err := st.BackupsToExpire(ctx, now.Add(-retention))
		if err != nil {
			return err
		}
		for _, b := range old {
			if _, err := st.BeginDeleteBackup(ctx, b.ProjectID, b.ID); err != nil && !errors.Is(err, store.ErrConflict) {
				log.Warn("retention", "backup", b.ID, "err", err)
			}
		}
	}
	del, err := st.DeletingBackups(ctx)
	if err != nil {
		return err
	}
	for _, b := range del {
		n, err := objs.DeletePrefix(ctx, b.Path+"/")
		if err != nil {
			log.Warn("deleting backup objects", "backup", b.ID, "err", err)
			continue
		}
		if err := st.MarkBackupState(ctx, b.ID, model.BackupDeleted); err != nil {
			return err
		}
		_ = st.Audit(ctx, "retention", "backup.deleted", b.ClusterID, b.ID, map[string]any{"objects": n})
		log.Info("backup deleted", "backup", b.ID, "objects", n)
	}
	return nil
}
