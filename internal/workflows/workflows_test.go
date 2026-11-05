package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/testsuite"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/engine/clickhouse"
	"github.com/avilashs/clickhouse_as_service/internal/model"
	"github.com/avilashs/clickhouse_as_service/internal/secrets"
	"github.com/avilashs/clickhouse_as_service/internal/store"
)

// These tests run the real workflows and activities against PostgreSQL (BASALT_TEST_PG_DSN)
// in Temporal's test environment. The test plays the cell agent by writing observed state.

func testStore(t *testing.T, sharedMilliCPU int64) *store.Store {
	t.Helper()
	dsn := os.Getenv("BASALT_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("BASALT_TEST_PG_DSN not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := fmt.Sprintf("basalt_wf_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		admin.Exec(context.Background(), "DROP DATABASE "+db+" WITH (FORCE)")
		admin.Close(context.Background())
	})
	cfg := admin.Config()
	s, err := store.Open(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, db))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	cell := model.Cell{ID: "cell1", Region: "local", State: "ACTIVE", Profile: "local", Domain: "db.example.com"}
	if err := s.EnsureCell(ctx, cell, 10, "x", []store.CellCapacity{{Pool: "shared", MilliCPU: sharedMilliCPU, MemoryBytes: 64 << 30, Pods: 20}}); err != nil {
		t.Fatal(err)
	}
	return s
}

func seedCluster(t *testing.T, s *store.Store) (model.Cluster, model.Operation) {
	t.Helper()
	id := model.NewID("c")
	c := model.Cluster{ID: id, ProjectID: "p1", CellID: "cell1", Name: "wf-" + id, State: model.ClusterCreating,
		Spec: model.ClusterSpec{Version: 1, Engine: "clickhouse", Tier: model.TierShared, Size: "t-2", Shards: 1, ReplicasPerShard: 2, StorageGiB: 5, ServerVersion: "26.8.34.13"}}
	op := model.Operation{ID: model.NewID("o"), ProjectID: "p1", ClusterID: c.ID, Kind: model.OpCreateCluster, State: model.OpPending}
	if _, _, err := s.CreateCluster(context.Background(), store.CreateClusterRequest{Cluster: c, Operation: op, IdempotencyKey: "k-" + op.ID, RequestHash: "h"}); err != nil {
		t.Fatal(err)
	}
	return c, op
}

func env(t *testing.T, s *store.Store) *testsuite.TestWorkflowEnvironment {
	var ts testsuite.WorkflowTestSuite
	e := ts.NewTestWorkflowEnvironment()
	sec, err := secrets.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	e.RegisterActivity(&Activities{Store: s, Secrets: sec, Engines: map[string]engine.Engine{"clickhouse": clickhouse.Engine{}}, PollInterval: 50 * time.Millisecond})
	return e
}

// fakeAgent reports, for every desired row, a namespace that exists (healthy or not) or is
// gone for tombstones, the way the real agent does.
func fakeAgent(ctx context.Context, t *testing.T, s *store.Store, healthy bool) {
	fakeAgentFn(ctx, t, s, func(store.DesiredRow) bool { return healthy })
}

// fakeAgentFn lets a test decide, per desired payload, whether the cell reaches it: for a payload
// it rejects, the pods keep running a spec that is not the desired one.
func fakeAgentFn(ctx context.Context, t *testing.T, s *store.Store, reaches func(store.DesiredRow) bool) {
	go func() {
		for ctx.Err() == nil {
			rows, _, err := s.DesiredForCell(ctx, "cell1")
			if err == nil {
				var reports []engine.Observed
				for _, r := range rows {
					o := engine.Observed{ClusterID: r.ClusterID, AppliedGeneration: r.Generation, AppliedHash: r.Hash, ObservedAt: time.Now()}
					if !r.Tombstone {
						o.NamespaceExists = true
						if reaches(r) {
							fillHealthy(&o, r, true)
						}
					}
					reports = append(reports, o)
				}
				_ = s.RecordObserved(ctx, "cell1", reports)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
}

// fillHealthy makes o the report of a cell in which every pod runs exactly what the desired
// payload asks for. keeperOK false makes the first replica lose its Keeper session.
func fillHealthy(o *engine.Observed, r store.DesiredRow, keeperOK bool) {
	var d engine.Desired
	_ = json.Unmarshal(r.Payload, &d)
	o.BootstrapHash = r.Hash
	o.PodsReady, o.PodsTotal = d.Expect.Pods, d.Expect.Pods
	o.Objects = map[string]string{"ClickHouseKeeperInstallation/" + r.ClusterID: "Completed", "ClickHouseInstallation/" + r.ClusterID: "Completed"}
	hosts := 0
	for _, role := range d.Expect.Roles {
		for i := 0; i < role.Count; i++ {
			o.PodList = append(o.PodList, engine.PodInfo{Name: fmt.Sprintf("%s-%d", role.Role, i), Role: role.Role, Image: role.Image, CPU: role.CPU, Memory: role.Memory, Ready: true})
		}
		if role.Role == clickhouse.ComponentServer {
			hosts = role.Count
		}
	}
	for i := 0; i < hosts; i++ {
		o.Probes = append(o.Probes, engine.HostProbe{Host: fmt.Sprint(i), OK: true, ClusterHosts: hosts, KeeperOK: keeperOK || i > 0, Version: d.Expect.ServerVersion})
	}
}

func TestCreateHappyPath(t *testing.T) {
	s := testStore(t, 8000)
	c, op := seedCluster(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fakeAgent(ctx, t, s, true)
	e := env(t, s)
	e.ExecuteWorkflow(CreateCluster, Input{OperationID: op.ID, ClusterID: c.ID})
	if !e.IsWorkflowCompleted() || e.GetWorkflowError() != nil {
		t.Fatalf("workflow: %v", e.GetWorkflowError())
	}
	got, _ := s.GetCluster(ctx, "", c.ID)
	o, _ := s.GetOperation(ctx, op.ID)
	if got.State != model.ClusterActive || o.State != model.OpSucceeded {
		t.Fatalf("cluster %s, operation %s", got.State, o.State)
	}
	if n, _ := s.Count(ctx, "capacity_reservations", "cluster_id=$1 AND state='COMMITTED'", c.ID); n != 1 {
		t.Fatalf("committed reservations: %d", n)
	}
}

func TestCreateCompensatesOnConvergenceTimeout(t *testing.T) {
	s := testStore(t, 8000)
	c, op := seedCluster(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fakeAgent(ctx, t, s, false) // the cluster never becomes healthy
	old := ConvergeTimeout
	ConvergeTimeout = 2 * time.Second
	defer func() { ConvergeTimeout = old }()
	e := env(t, s)
	e.ExecuteWorkflow(CreateCluster, Input{OperationID: op.ID, ClusterID: c.ID})
	if e.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to fail")
	}
	got, _ := s.GetCluster(ctx, "", c.ID)
	o, _ := s.GetOperation(ctx, op.ID)
	if got.State != model.ClusterFailed || o.State != model.OpFailed {
		t.Fatalf("cluster %s, operation %s", got.State, o.State)
	}
	row, err := s.GetDesired(ctx, c.ID)
	if err != nil || !row.Tombstone {
		t.Fatalf("compensation must tombstone the desired state: %+v %v", row, err)
	}
	if n, _ := s.Count(ctx, "capacity_reservations", "cluster_id=$1 AND state<>'RELEASED'", c.ID); n != 0 {
		t.Fatalf("capacity not released after confirmed teardown: %d", n)
	}
}

func TestCreateFailsCleanlyWithoutCapacity(t *testing.T) {
	s := testStore(t, 1000) // a t-2 cluster needs 2000m
	c, op := seedCluster(t, s)
	e := env(t, s)
	e.ExecuteWorkflow(CreateCluster, Input{OperationID: op.ID, ClusterID: c.ID})
	if e.GetWorkflowError() == nil {
		t.Fatal("expected a capacity failure")
	}
	ctx := context.Background()
	o, _ := s.GetOperation(ctx, op.ID)
	if o.State != model.OpFailed {
		t.Fatalf("operation %s", o.State)
	}
	if _, err := s.GetDesired(ctx, c.ID); err != store.ErrNotFound {
		t.Fatalf("nothing may be published when capacity is missing: %v", err)
	}
	if n, _ := s.Count(ctx, "capacity_reservations", "cluster_id=$1", c.ID); n != 0 {
		t.Fatalf("reservations: %d", n)
	}
}

// ---------------------------------------------------------------- updates

func startUpdate(t *testing.T, s *store.Store, c model.Cluster, mutate func(*model.ClusterSpec)) model.Operation {
	t.Helper()
	cur, err := s.GetCluster(context.Background(), "", c.ID)
	if err != nil {
		t.Fatal(err)
	}
	next := cur.Spec
	mutate(&next)
	op := model.Operation{ID: model.NewID("o"), ProjectID: "p1", ClusterID: c.ID, Kind: model.OpUpdateCluster, State: model.OpPending}
	op, _, err = s.StartUpdate(context.Background(), "p1", cur.Spec.Version, next, op, "upd-"+op.ID, "h-"+op.ID)
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func createActive(t *testing.T, s *store.Store) model.Cluster {
	t.Helper()
	c, op := seedCluster(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	fakeAgent(ctx, t, s, true)
	e := env(t, s)
	e.ExecuteWorkflow(CreateCluster, Input{OperationID: op.ID, ClusterID: c.ID})
	cancel()
	if e.GetWorkflowError() != nil {
		t.Fatalf("create: %v", e.GetWorkflowError())
	}
	return c
}

func TestUpdateScalesAndSwapsTheReservation(t *testing.T) {
	s := testStore(t, 12000)
	c := createActive(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fakeAgent(ctx, t, s, true)
	op := startUpdate(t, s, c, func(sp *model.ClusterSpec) { sp.Size, sp.ReplicasPerShard = "t-3r", 3 })
	if got, _ := s.GetCluster(ctx, "", c.ID); got.State != model.ClusterUpdating {
		t.Fatalf("state %s, want UPDATING", got.State)
	}
	e := env(t, s)
	e.ExecuteWorkflow(UpdateCluster, Input{OperationID: op.ID, ClusterID: c.ID})
	if !e.IsWorkflowCompleted() || e.GetWorkflowError() != nil {
		t.Fatalf("workflow: %v", e.GetWorkflowError())
	}
	got, _ := s.GetCluster(ctx, "", c.ID)
	o, _ := s.GetOperation(ctx, op.ID)
	if got.State != model.ClusterActive || o.State != model.OpSucceeded || got.Spec.Size != "t-3r" {
		t.Fatalf("cluster %s size %s, operation %s", got.State, got.Spec.Size, o.State)
	}
	cur, _ := s.CurrentSpec(ctx, c.ID)
	if cur.Size != "t-3r" {
		t.Fatalf("current spec %s", cur.Size)
	}
	// the cluster holds exactly the t-3r footprint: three replicas of 1 CPU
	if n, _ := s.Count(ctx, "capacity_reservations", "cluster_id=$1 AND state='COMMITTED' AND milli_cpu=3000 AND pods=3", c.ID); n != 1 {
		t.Fatalf("committed reservations with the new footprint: %d", n)
	}
	if n, _ := s.Count(ctx, "capacity_reservations", "cluster_id=$1 AND state<>'RELEASED'", c.ID); n != 1 {
		t.Fatalf("live reservations: %d, want exactly one", n)
	}
}

func TestUpdateRollsBackWhenItDoesNotConverge(t *testing.T) {
	s := testStore(t, 12000)
	c := createActive(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// the cell reaches the original spec but never the new one (pods stay on the old image)
	orig, _ := s.GetDesired(ctx, c.ID)
	fakeAgentFn(ctx, t, s, func(r store.DesiredRow) bool { return r.Hash == orig.Hash })
	oldU, oldR := UpdateTimeout, RollbackTimeout
	UpdateTimeout, RollbackTimeout = 2*time.Second, 10*time.Second
	defer func() { UpdateTimeout, RollbackTimeout = oldU, oldR }()
	op := startUpdate(t, s, c, func(sp *model.ClusterSpec) { sp.ServerVersion = "26.8.36.3" })
	e := env(t, s)
	e.ExecuteWorkflow(UpdateCluster, Input{OperationID: op.ID, ClusterID: c.ID})
	if e.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to fail")
	}
	got, _ := s.GetCluster(ctx, "", c.ID)
	o, _ := s.GetOperation(ctx, op.ID)
	if o.State != model.OpFailed || !strings.Contains(o.Error, "rolled back") {
		t.Fatalf("operation %s: %q", o.State, o.Error)
	}
	if got.State != model.ClusterActive || got.Spec.ServerVersion != "26.8.34.13" {
		t.Fatalf("cluster %s on %s, want ACTIVE on the original version", got.State, got.Spec.ServerVersion)
	}
	row, _ := s.GetDesired(ctx, c.ID)
	if row.Hash != orig.Hash {
		t.Fatal("the desired state was not put back")
	}
	if n, _ := s.Count(ctx, "capacity_reservations", "cluster_id=$1 AND state<>'RELEASED'", c.ID); n != 1 {
		t.Fatalf("live reservations after rollback: %d, want the original one", n)
	}
}

func TestUpdateRefusedWithoutCapacityChangesNothing(t *testing.T) {
	s := testStore(t, 2500) // one t-2 cluster takes 2000m; t-3r needs 1000m more
	c := createActive(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fakeAgent(ctx, t, s, true)
	op := startUpdate(t, s, c, func(sp *model.ClusterSpec) { sp.Size, sp.ReplicasPerShard = "t-3r", 3 })
	e := env(t, s)
	e.ExecuteWorkflow(UpdateCluster, Input{OperationID: op.ID, ClusterID: c.ID})
	if e.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to fail")
	}
	got, _ := s.GetCluster(ctx, "", c.ID)
	o, _ := s.GetOperation(ctx, op.ID)
	if got.State != model.ClusterActive || got.Spec.Size != "t-2" || o.State != model.OpFailed {
		t.Fatalf("cluster %s on %s, operation %s", got.State, got.Spec.Size, o.State)
	}
}

func TestStartUpdateRules(t *testing.T) {
	s := testStore(t, 12000)
	c, _ := seedCluster(t, s) // still CREATING
	cur, _ := s.GetCluster(context.Background(), "", c.ID)
	op := model.Operation{ID: model.NewID("o"), ProjectID: "p1", ClusterID: c.ID, Kind: model.OpUpdateCluster, State: model.OpPending}
	if _, _, err := s.StartUpdate(context.Background(), "p1", cur.Spec.Version, cur.Spec, op, "k1", "h1"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("a CREATING cluster must refuse updates: %v", err)
	}
	a := createActive(t, s)
	got, _ := s.GetCluster(context.Background(), "", a.ID)
	op2 := model.Operation{ID: model.NewID("o"), ProjectID: "p1", ClusterID: a.ID, Kind: model.OpUpdateCluster, State: model.OpPending}
	if _, _, err := s.StartUpdate(context.Background(), "p1", got.Spec.Version+7, got.Spec, op2, "k2", "h2"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("a stale base version must be refused: %v", err)
	}
	if _, _, err := s.StartUpdate(context.Background(), "other-project", got.Spec.Version, got.Spec, op2, "k3", "h3"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("another project's cluster must look absent: %v", err)
	}
}
