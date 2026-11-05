package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/avilashs/clickhouse_as_service/internal/api"
	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

// Backup and restore jobs. The control plane never moves backup data: the cell agent asks the
// cluster's own server to run a native BACKUP or RESTORE against the object store (ADR-0006), with
// credentials the control plane limited to this one backup. The credentials reach the server in a
// named collection that exists for the duration of the job; tenants hold no grant on named
// collections, so they can neither read the collection nor name it in a query.

// jobState is what the agent knows about a job it started. It lives in memory only: if the agent
// restarts while a job runs, the job is reported FAILED (the control plane retries on its
// schedule) rather than started a second time against a half-written backup.
type jobState struct {
	state   string // RUNNING | SUCCEEDED | FAILED
	message string
	result  json.RawMessage
	seen    time.Time
}

const (
	jobPoll         = 3 * time.Second
	replicaCatchUp  = 15 * time.Minute
	jobReadTimeout  = 30 * time.Minute // a single CHECK TABLE or fingerprint on a large table
	collectionMaxMS = 10_000
)

// runJobs starts the jobs the control plane has handed over and returns the report for every job
// it knows. It does not block on a running job.
func (a *Agent) runJobs(ctx context.Context, jobs []api.JobEntry, clusters []api.DesiredEntry) []api.JobReport {
	a.jobMu.Lock()
	defer a.jobMu.Unlock()
	if a.jobs == nil {
		a.jobs = map[string]*jobState{}
	}
	var reports []api.JobReport
	now := time.Now()
	for _, j := range jobs {
		st, known := a.jobs[j.ID]
		if !known {
			st = &jobState{state: "RUNNING"}
			a.jobs[j.ID] = st
			switch {
			case j.State != "PENDING":
				// we reported it RUNNING before, so an earlier agent started it
				st.state, st.message = "FAILED", "the cell agent restarted while the job ran"
			default:
				var entry *api.DesiredEntry
				for i := range clusters {
					if clusters[i].ClusterID == j.ClusterID && !clusters[i].Tombstone {
						entry = &clusters[i]
					}
				}
				if entry == nil || entry.Desired.SQL == nil {
					st.state, st.message = "FAILED", "the cluster is not in this cell's desired state"
				} else {
					d := entry.Desired
					go a.execJob(ctx, j, d)
				}
			}
		}
		st.seen = now
		reports = append(reports, api.JobReport{ID: j.ID, State: st.state, Message: st.message, Result: st.result})
	}
	// forget jobs the control plane no longer serves
	for id, st := range a.jobs {
		if now.Sub(st.seen) > 10*time.Minute {
			delete(a.jobs, id)
		}
	}
	return reports
}

func (a *Agent) finishJob(id, state, msg string, result any) {
	var raw json.RawMessage
	if result != nil {
		raw, _ = json.Marshal(result)
	}
	a.jobMu.Lock()
	defer a.jobMu.Unlock()
	if st := a.jobs[id]; st != nil {
		st.state, st.message, st.result = state, msg, raw
	}
}

func (a *Agent) execJob(ctx context.Context, j api.JobEntry, d engine.Desired) {
	log := a.Log.With("job", j.ID, "cluster", j.ClusterID, "action", j.Spec.Action)
	log.Info("job started")
	var res model.JobResult
	var err error
	switch j.Spec.Action {
	case "backup":
		res, err = a.backupJob(ctx, j, d)
	case "restore":
		res, err = a.restoreJob(ctx, j, d)
	default:
		err = fmt.Errorf("unknown job action %q", j.Spec.Action)
	}
	if err != nil {
		log.Warn("job failed", "err", err)
		a.finishJob(j.ID, "FAILED", scrub(err.Error(), j.Spec), nil)
		return
	}
	log.Info("job succeeded", "tables", res.Tables)
	a.finishJob(j.ID, "SUCCEEDED", "", res)
}

// scrub removes the credentials from a message before it is stored or shown.
func scrub(msg string, s model.JobSpec) string {
	for _, secret := range []string{s.SecretAccessKey, s.SessionToken} {
		if secret != "" {
			msg = strings.ReplaceAll(msg, secret, "[HIDDEN]")
		}
	}
	return msg
}

func sqlString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

func collectionName(jobID string) string {
	return "basalt_job_" + strings.NewReplacer("-", "_").Replace(jobID)
}

// withCollection creates the job's named collection on the host, runs fn and drops them again.
// Base backups of an incremental backup get collections of their own (model.BaseCollection):
// overriding the url of one collection in the statement would need the grant to see the
// collection's secrets, and a restore must find each base under the name the backup recorded.
func withCollection(ctx context.Context, c clickhouse.Conn, j api.JobEntry, fn func(name string, bases []string) error) error {
	var made []string
	defer func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		for _, n := range made {
			_ = c.Exec(dctx, "DROP NAMED COLLECTION IF EXISTS "+n)
		}
	}()
	create := func(name, url string) error {
		made = append(made, name)
		// a collection left by a crashed earlier attempt of the same job is replaced
		_ = c.Exec(ctx, "DROP NAMED COLLECTION IF EXISTS "+name)
		stmt := fmt.Sprintf("CREATE NAMED COLLECTION %s AS url = %s, access_key_id = %s, secret_access_key = %s",
			name, sqlString(url), sqlString(j.Spec.AccessKeyID), sqlString(j.Spec.SecretAccessKey))
		if j.Spec.SessionToken != "" {
			stmt += ", session_token = " + sqlString(j.Spec.SessionToken)
		}
		if err := c.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("create the job's named collection: %s", scrub(err.Error(), j.Spec))
		}
		return nil
	}
	name := collectionName(j.ID)
	if err := create(name, j.Spec.URL); err != nil {
		return err
	}
	var bases []string
	for _, b := range j.Spec.Bases {
		bn := model.BaseCollection(b.ID)
		if err := create(bn, b.URL); err != nil {
			return err
		}
		bases = append(bases, bn)
	}
	return fn(name, bases)
}

// userDatabases are the databases a backup covers: everything except the system ones.
func userDatabases(ctx context.Context, c clickhouse.Conn) ([]string, error) {
	rows, err := c.Query(ctx, "SELECT name FROM system.databases WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

type tableRef struct{ db, name, engine string }

func (t tableRef) full() string { return quoteIdent(t.db) + "." + quoteIdent(t.name) }
func (t tableRef) key() string  { return t.db + "." + t.name }

// mergeTreeTables lists the tables whose contents a fingerprint can cover.
func mergeTreeTables(ctx context.Context, c clickhouse.Conn, dbs []string) ([]tableRef, error) {
	rows, err := c.Query(ctx, "SELECT database, name, engine FROM system.tables WHERE database IN ? AND engine LIKE '%MergeTree' AND NOT is_temporary ORDER BY database, name", dbs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []tableRef
	for rows.Next() {
		var t tableRef
		if err := rows.Scan(&t.db, &t.name, &t.engine); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// fingerprint counts a table's rows and hashes their content, independent of row order and of
// how the rows are split into parts.
func fingerprint(ctx context.Context, c clickhouse.Conn, t tableRef) (model.TableFingerprint, error) {
	var rows uint64
	var hash string
	err := c.QueryRow(ctx, fmt.Sprintf("SELECT count(), toString(sum(cityHash64(*))) FROM %s", t.full())).Scan(&rows, &hash)
	return model.TableFingerprint{Rows: rows, Hash: hash}, err
}

func fingerprints(ctx context.Context, c clickhouse.Conn, tables []tableRef) (map[string]model.TableFingerprint, error) {
	out := map[string]model.TableFingerprint{}
	for _, t := range tables {
		f, err := fingerprint(ctx, c, t)
		if err != nil {
			return nil, fmt.Errorf("fingerprint of %s: %w", t.key(), err)
		}
		out[t.key()] = f
	}
	return out, nil
}

// syncReplicas waits until the host has applied everything its replication queue holds for the
// tables, so that a backup (or a fingerprint) sees what the cluster has acknowledged.
func syncReplicas(ctx context.Context, c clickhouse.Conn, tables []tableRef) error {
	for _, t := range tables {
		if !strings.HasPrefix(t.engine, "Replicated") {
			continue
		}
		sctx, cancel := context.WithTimeout(ctx, replicaCatchUp)
		err := c.Exec(sctx, "SYSTEM SYNC REPLICA "+t.full())
		cancel()
		if err != nil {
			return fmt.Errorf("sync replica %s: %w", t.key(), err)
		}
	}
	return nil
}

// waitBackup polls system.backups until the operation with this id ends.
func waitBackup(ctx context.Context, c clickhouse.Conn, id string, okStatus string) error {
	for {
		var status, errText string
		err := c.QueryRow(ctx, "SELECT status, error FROM system.backups WHERE id = ?", id).Scan(&status, &errText)
		switch {
		case err != nil:
			return fmt.Errorf("reading the status of backup operation %s: %w", id, err)
		case status == okStatus:
			return nil
		case strings.HasSuffix(status, "FAILED") || strings.HasSuffix(status, "CANCELLED"):
			return fmt.Errorf("%s: %s", status, errText)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jobPoll):
		}
	}
}

func (a *Agent) backupJob(ctx context.Context, j api.JobEntry, d engine.Desired) (model.JobResult, error) {
	var res model.JobResult
	c, err := a.connRead(d, d.SQL.Hosts[0], jobReadTimeout)
	if err != nil {
		return res, err
	}
	defer c.Close()
	dbs, err := userDatabases(ctx, c)
	if err != nil {
		return res, err
	}
	tables, err := mergeTreeTables(ctx, c, dbs)
	if err != nil {
		return res, err
	}
	if err := syncReplicas(ctx, c, tables); err != nil {
		return res, err
	}
	if res.Before, err = fingerprints(ctx, c, tables); err != nil {
		return res, err
	}
	targets := make([]string, 0, len(dbs))
	for _, db := range dbs {
		targets = append(targets, "DATABASE "+quoteIdent(db))
	}
	err = withCollection(ctx, c, j, func(nc string, bases []string) error {
		stmt := fmt.Sprintf("BACKUP %s TO S3(%s)", strings.Join(targets, ", "), nc)
		if len(bases) > 0 {
			// the base backup is read with the same credentials: they cover its prefix as well
			stmt += fmt.Sprintf(" SETTINGS base_backup = S3(%s)", bases[0])
		}
		var id, status string
		if err := c.QueryRow(ctx, stmt+" ASYNC").Scan(&id, &status); err != nil {
			return fmt.Errorf("BACKUP: %s", scrub(err.Error(), j.Spec))
		}
		return waitBackup(ctx, c, id, "BACKUP_CREATED")
	})
	if err != nil {
		return res, err
	}
	if res.After, err = fingerprints(ctx, c, tables); err != nil {
		return res, err
	}
	res.Tables = len(tables)
	return res, nil
}

func (a *Agent) restoreJob(ctx context.Context, j api.JobEntry, d engine.Desired) (model.JobResult, error) {
	var res model.JobResult
	c, err := a.connRead(d, d.SQL.Hosts[0], jobReadTimeout)
	if err != nil {
		return res, err
	}
	defer c.Close()
	err = withCollection(ctx, c, j, func(nc string, _ []string) error {
		var id, status string
		if err := c.QueryRow(ctx, fmt.Sprintf("RESTORE ALL FROM S3(%s) ASYNC", nc)).Scan(&id, &status); err != nil {
			return fmt.Errorf("RESTORE: %s", scrub(err.Error(), j.Spec))
		}
		return waitBackup(ctx, c, id, "RESTORED")
	})
	if err != nil {
		return res, err
	}
	dbs, err := userDatabases(ctx, c)
	if err != nil {
		return res, err
	}
	tables, err := mergeTreeTables(ctx, c, dbs)
	if err != nil {
		return res, err
	}
	// The other replicas learn the tables through the schema sync and fetch their parts; wait
	// until each has the same tables and has caught up, so the cluster is whole when we report.
	if err := a.waitReplicasHold(ctx, d, tables); err != nil {
		return res, err
	}
	if err := syncReplicas(ctx, c, tables); err != nil {
		return res, err
	}
	if res.Restored, err = fingerprints(ctx, c, tables); err != nil {
		return res, err
	}
	res.Checks = map[string]string{}
	for _, t := range tables {
		res.Checks[t.key()] = checkTable(ctx, c, t)
	}
	res.Tables = len(tables)
	return res, nil
}

// waitReplicasHold waits until every replica has every restored table, then until each has caught
// up on it.
func (a *Agent) waitReplicasHold(ctx context.Context, d engine.Desired, tables []tableRef) error {
	if len(d.SQL.Hosts) < 2 || len(tables) == 0 {
		return nil
	}
	deadline := time.Now().Add(replicaCatchUp)
	want := make([]string, 0, len(tables))
	for _, t := range tables {
		want = append(want, t.key())
	}
	sort.Strings(want)
	for _, h := range d.SQL.Hosts[1:] {
		for {
			missing, err := a.missingTables(ctx, d, h, want)
			if err == nil && len(missing) == 0 {
				break
			}
			if time.Now().After(deadline) {
				if err != nil {
					return fmt.Errorf("replica %s: %w", h, err)
				}
				return fmt.Errorf("replica %s still lacks %d table(s) after %v, e.g. %s", h, len(missing), replicaCatchUp, missing[0])
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(jobPoll):
			}
		}
		c, err := a.connRead(d, h, jobReadTimeout)
		if err != nil {
			return err
		}
		err = syncReplicas(ctx, c, tables)
		c.Close()
		if err != nil {
			return fmt.Errorf("replica %s: %w", h, err)
		}
	}
	return nil
}

func (a *Agent) missingTables(ctx context.Context, d engine.Desired, host string, want []string) ([]string, error) {
	c, err := a.conn(d, host)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	qctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	rows, err := c.Query(qctx, "SELECT concat(database, '.', name) FROM system.tables WHERE NOT is_temporary")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	have := map[string]bool{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		have[k] = true
	}
	var missing []string
	for _, k := range want {
		if !have[k] {
			missing = append(missing, k)
		}
	}
	return missing, rows.Err()
}

// checkTable runs CHECK TABLE and returns "" if every part passed, else what was wrong.
func checkTable(ctx context.Context, c clickhouse.Conn, t tableRef) string {
	rows, err := c.Query(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"check_query_single_value_result": 0})), "CHECK TABLE "+t.full())
	if err != nil {
		return err.Error()
	}
	defer rows.Close()
	var bad []string
	n := 0
	for rows.Next() {
		var part string
		var passed uint8
		var msg string
		if err := rows.Scan(&part, &passed, &msg); err != nil {
			return err.Error()
		}
		n++
		if passed == 0 {
			bad = append(bad, part+": "+msg)
		}
	}
	if err := rows.Err(); err != nil {
		return err.Error()
	}
	if len(bad) > 0 {
		if len(bad) > 3 {
			bad = append(bad[:3], fmt.Sprintf("and %d more", len(bad)-3))
		}
		return strings.Join(bad, "; ")
	}
	return ""
}
