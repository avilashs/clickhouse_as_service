package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
)

// A replica that joins a cluster (add replica, or a replacement after its volume was lost) starts
// without the tenant's databases, tables, views and functions. The Altinity operator logs that it
// migrates the schema but did not do so for tables a tenant created in the default database (M5:
// "Tables added successfully", then UNKNOWN_TABLE on the new replica), and a replica that is
// silently missing its tables is exactly the loss of high availability the platform exists to
// prevent. So the agent copies the schema itself.
//
// It only ever writes to a replica that has just started (uptime below freshReplica), and only
// objects that every other healthy replica has. A table dropped ON CLUSTER is gone from some
// replicas before others, so it is never recreated by this path.
const (
	freshReplica = time.Hour
	syncEvery    = 30 * time.Second
)

var systemDatabases = []string{"system", "INFORMATION_SCHEMA", "information_schema"}

type schemaObject struct {
	Database, Name, Engine, Create string
}

func (o schemaObject) key() string { return o.Database + "." + o.Name }

// kindRank orders creation: tables other objects read from come first.
func (o schemaObject) rank() int {
	switch {
	case strings.HasPrefix(o.Engine, "Replicated"):
		return 0
	case o.Engine == "Distributed":
		return 1
	case o.Engine == "Dictionary":
		return 2
	case o.Engine == "View":
		return 3
	case o.Engine == "MaterializedView":
		return 4
	}
	return 5
}

const schemaObjectsSQL = `
SELECT database, name, engine, create_table_query FROM system.tables
WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
  AND database IN (SELECT name FROM system.databases WHERE engine = 'Atomic')
  AND NOT is_temporary AND NOT startsWith(name, '.inner')
  AND (engine LIKE 'Replicated%' OR engine IN ('Distributed', 'Dictionary', 'View', 'MaterializedView'))
SETTINGS show_table_uuid_in_table_create_query_if_not_nil = 1`

type hostSchema struct {
	host      string
	uptime    time.Duration
	databases []string
	objects   map[string]schemaObject
	functions map[string]string // name -> CREATE FUNCTION
}

func (a *Agent) readSchema(ctx context.Context, d engine.Desired, host string) (hostSchema, error) {
	hs := hostSchema{host: host, objects: map[string]schemaObject{}, functions: map[string]string{}}
	c, err := a.conn(d, host)
	if err != nil {
		return hs, err
	}
	defer c.Close()
	qctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var up uint32
	if err := c.QueryRow(qctx, "SELECT uptime()").Scan(&up); err != nil {
		return hs, err
	}
	hs.uptime = time.Duration(up) * time.Second
	rows, err := c.Query(qctx, "SELECT name FROM system.databases WHERE engine = 'Atomic' AND name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')")
	if err != nil {
		return hs, err
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return hs, err
		}
		hs.databases = append(hs.databases, n)
	}
	rows.Close()
	rows, err = c.Query(qctx, schemaObjectsSQL)
	if err != nil {
		return hs, err
	}
	for rows.Next() {
		var o schemaObject
		if err := rows.Scan(&o.Database, &o.Name, &o.Engine, &o.Create); err != nil {
			rows.Close()
			return hs, err
		}
		hs.objects[o.key()] = o
	}
	rows.Close()
	// SQL user-defined functions are stored per server; a missing column or table is not an error
	if frows, err := c.Query(qctx, "SELECT name, create_query FROM system.functions WHERE origin = 'SQLUserDefined'"); err == nil {
		for frows.Next() {
			var n, q string
			if frows.Scan(&n, &q) == nil {
				hs.functions[n] = q
			}
		}
		frows.Close()
	}
	return hs, nil
}

// idempotent turns a CREATE statement into its IF NOT EXISTS form.
func idempotent(create string) string {
	for _, p := range []string{"CREATE MATERIALIZED VIEW ", "CREATE DICTIONARY ", "CREATE VIEW ", "CREATE TABLE ", "CREATE FUNCTION "} {
		if strings.HasPrefix(create, p) {
			return p + "IF NOT EXISTS " + strings.TrimPrefix(create, p)
		}
	}
	return create
}

// planSchemaSync works out what target is missing that every other healthy replica has.
func planSchemaSync(target hostSchema, others []hostSchema) (dbs []string, objs []schemaObject, funcs []string) {
	if len(others) == 0 {
		return
	}
	have := map[string]bool{}
	for _, d := range target.databases {
		have[d] = true
	}
	for _, d := range others[0].databases {
		inAll := true
		for _, o := range others[1:] {
			found := false
			for _, od := range o.databases {
				found = found || od == d
			}
			inAll = inAll && found
		}
		if inAll && !have[d] {
			dbs = append(dbs, d)
		}
	}
	for k, o := range others[0].objects {
		inAll := true
		for _, other := range others[1:] {
			_, ok := other.objects[k]
			inAll = inAll && ok
		}
		if _, ok := target.objects[k]; inAll && !ok {
			objs = append(objs, o)
		}
	}
	sort.Slice(objs, func(i, j int) bool {
		if objs[i].rank() != objs[j].rank() {
			return objs[i].rank() < objs[j].rank()
		}
		return objs[i].key() < objs[j].key()
	})
	for n := range others[0].functions {
		inAll := true
		for _, other := range others[1:] {
			_, ok := other.functions[n]
			inAll = inAll && ok
		}
		if _, ok := target.functions[n]; inAll && !ok {
			funcs = append(funcs, n)
		}
	}
	sort.Strings(funcs)
	return
}

// syncNewReplicas copies the schema to replicas that have just started. It returns how many
// objects it created.
func (a *Agent) syncNewReplicas(ctx context.Context, d engine.Desired, log *slog.Logger) int {
	if len(d.SQL.Hosts) < 2 {
		return 0
	}
	schemas := make([]hostSchema, 0, len(d.SQL.Hosts))
	for _, h := range d.SQL.Hosts {
		hs, err := a.readSchema(ctx, d, h)
		if err != nil {
			return 0 // every replica must answer before anything is copied
		}
		schemas = append(schemas, hs)
	}
	created := 0
	for i, target := range schemas {
		if target.uptime >= freshReplica {
			continue
		}
		var others []hostSchema
		for j, s := range schemas {
			if j != i {
				others = append(others, s)
			}
		}
		dbs, objs, funcs := planSchemaSync(target, others)
		if len(dbs)+len(objs)+len(funcs) == 0 {
			continue
		}
		n, err := a.applySchema(ctx, d, target.host, dbs, objs, funcs, others[0])
		created += n
		if err != nil {
			log.Warn("schema sync", "host", target.host, "created", n, "err", err)
		} else {
			log.Info("schema copied to a new replica", "host", target.host, "databases", len(dbs), "objects", len(objs), "functions", len(funcs))
		}
	}
	return created
}

func (a *Agent) applySchema(ctx context.Context, d engine.Desired, host string, dbs []string, objs []schemaObject, funcs []string, src hostSchema) (int, error) {
	c, err := a.conn(d, host)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	exec := func(q string) error {
		qctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		return c.Exec(qctx, q)
	}
	n := 0
	for _, db := range dbs {
		if err := exec("CREATE DATABASE IF NOT EXISTS " + quoteIdent(db)); err != nil {
			return n, fmt.Errorf("database %s: %w", db, err)
		}
		n++
	}
	// views and dictionaries can depend on tables created in the same pass: retry what failed
	pending := objs
	for pass := 0; pass < 6 && len(pending) > 0; pass++ {
		var next []schemaObject
		var lastErr error
		for _, o := range pending {
			if err := exec(idempotent(o.Create)); err != nil {
				next = append(next, o)
				lastErr = fmt.Errorf("%s: %w", o.key(), err)
				continue
			}
			n++
		}
		if len(next) == len(pending) {
			return n, lastErr
		}
		pending = next
	}
	if len(pending) > 0 {
		return n, fmt.Errorf("%d objects could not be created (%s ...)", len(pending), pending[0].key())
	}
	for _, f := range funcs {
		if q, ok := src.functions[f]; ok {
			if err := exec(idempotent(q)); err != nil {
				return n, fmt.Errorf("function %s: %w", f, err)
			}
			n++
		}
	}
	return n, nil
}

func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

var _ clickhouse.Conn
