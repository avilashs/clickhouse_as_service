package workflows

import (
	"context"
	"fmt"
	"os"
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
	c := model.Cluster{ID: model.NewID("c"), ProjectID: "p1", CellID: "cell1", Name: "wf", State: model.ClusterCreating,
		Spec: model.ClusterSpec{Version: 1, Engine: "clickhouse", Tier: model.TierShared, Size: "t-2", Shards: 1, ReplicasPerShard: 2, StorageGiB: 5, ServerVersion: "26.8.34.13"}}
	op := model.Operation{ID: model.NewID("o"), ProjectID: "p1", ClusterID: c.ID, Kind: model.OpCreateCluster, State: model.OpPending}
	if _, _, err := s.CreateCluster(context.Background(), store.CreateClusterRequest{Cluster: c, Operation: op, IdempotencyKey: "k", RequestHash: "h"}); err != nil {
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
	go func() {
		for ctx.Err() == nil {
			rows, _, err := s.DesiredForCell(ctx, "cell1")
			if err == nil {
				var reports []engine.Observed
				for _, r := range rows {
					o := engine.Observed{ClusterID: r.ClusterID, AppliedGeneration: r.Generation, AppliedHash: r.Hash, ObservedAt: time.Now()}
					if !r.Tombstone {
						o.NamespaceExists = true
						if healthy {
							o.BootstrapHash = r.Hash
							o.PodsReady, o.PodsTotal = 5, 5
							o.Objects = map[string]string{"ClickHouseKeeperInstallation/" + r.ClusterID: "Completed", "ClickHouseInstallation/" + r.ClusterID: "Completed"}
							o.Probes = []engine.HostProbe{{Host: "a", OK: true, ClusterHosts: 2, KeeperOK: true}, {Host: "b", OK: true, ClusterHosts: 2, KeeperOK: true}}
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
