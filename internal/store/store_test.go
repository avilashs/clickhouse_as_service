package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

// Integration tests: they need PostgreSQL (BASALT_TEST_PG_DSN) and run in a fresh database.
func testStore(t *testing.T) *Store {
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
	db := fmt.Sprintf("basalt_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		admin.Exec(context.Background(), "DROP DATABASE "+db+" WITH (FORCE)")
		admin.Close(context.Background())
	})
	cfg := admin.Config()
	s, err := Open(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, db))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil { // idempotent
		t.Fatal(err)
	}
	cell := model.Cell{ID: "cell1", Region: "local", State: "ACTIVE", Profile: "local", Domain: "db.example.com"}
	if err := s.EnsureCell(ctx, cell, 10, "x", []CellCapacity{{Pool: "shared", MilliCPU: 4000, MemoryBytes: 8 << 30, Pods: 4}}); err != nil {
		t.Fatal(err)
	}
	return s
}

func newCreate(project, name, key, hash string) CreateClusterRequest {
	c := model.Cluster{ID: model.NewID("c"), ProjectID: project, CellID: "cell1", Name: name, State: model.ClusterCreating,
		Spec: model.ClusterSpec{Version: 1, Engine: "clickhouse", Tier: model.TierShared, Size: "t-2", Shards: 1, ReplicasPerShard: 2, StorageGiB: 5, ServerVersion: "26.8.34.13"}}
	return CreateClusterRequest{Cluster: c, Operation: model.Operation{ID: model.NewID("o"), ProjectID: project, ClusterID: c.ID, Kind: model.OpCreateCluster, State: model.OpPending},
		IdempotencyKey: key, RequestHash: hash}
}

func TestIdempotentCreate(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	op1, created, err := s.CreateCluster(ctx, newCreate("p1", "orders", "k1", "h1"))
	if err != nil || !created {
		t.Fatalf("first create: %v %v", created, err)
	}
	// same key, same request: same operation, nothing new inserted (even with a new cluster ID)
	op2, created, err := s.CreateCluster(ctx, newCreate("p1", "orders", "k1", "h1"))
	if err != nil || created || op2.ID != op1.ID {
		t.Fatalf("replay: created=%v op=%s want %s err=%v", created, op2.ID, op1.ID, err)
	}
	if n, _ := s.Count(ctx, "clusters", "project_id = $1", "p1"); n != 1 {
		t.Fatalf("replay created %d clusters", n)
	}
	// same key, different request
	if _, _, err := s.CreateCluster(ctx, newCreate("p1", "orders2", "k1", "h2")); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("key reuse: %v", err)
	}
	// new key, same name
	if _, _, err := s.CreateCluster(ctx, newCreate("p1", "orders", "k2", "h3")); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate name: %v", err)
	}
	// keys are per project
	if _, created, err := s.CreateCluster(ctx, newCreate("p2", "orders", "k1", "h1")); err != nil || !created {
		t.Fatalf("other project, same key: %v", err)
	}
}

func TestOneActiveOperationPerCluster(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	op, _, err := s.CreateCluster(ctx, newCreate("p1", "orders", "k1", "h1"))
	if err != nil {
		t.Fatal(err)
	}
	del := model.Operation{ID: model.NewID("o"), ProjectID: "p1", ClusterID: op.ClusterID, Kind: model.OpDeleteCluster, State: model.OpPending}
	if _, _, err := s.StartOperation(ctx, del, "d1", "x"); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete during create must conflict: %v", err)
	}
	if err := s.SetOperationState(ctx, op.ID, model.OpSucceeded, ""); err != nil {
		t.Fatal(err)
	}
	if _, created, err := s.StartOperation(ctx, del, "d1", "x"); err != nil || !created {
		t.Fatalf("delete after create: %v", err)
	}
	// terminal states are sticky
	_ = s.SetOperationState(ctx, op.ID, model.OpRunning, "")
	if got, _ := s.GetOperation(ctx, op.ID); got.State != model.OpSucceeded {
		t.Fatalf("terminal state overwritten: %s", got.State)
	}
}

func TestReserveIsIdempotentAndBounded(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	plan := engine.PlacementPlan{Pool: "shared", MilliCPU: 2000, MemoryBytes: 4 << 30, Pods: 2}
	// retrying the same operation never double-reserves
	for i := 0; i < 3; i++ {
		if err := s.Reserve(ctx, "op-a", "cell1", "c-a", plan); err != nil {
			t.Fatal(err)
		}
	}
	if n, _ := s.Count(ctx, "capacity_reservations", "operation_id = $1", "op-a"); n != 1 {
		t.Fatalf("%d reservations for one operation", n)
	}
	// 4000m total: one more fits, then capacity is exhausted, even under concurrency
	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.Reserve(ctx, fmt.Sprintf("op-%d", i), "cell1", fmt.Sprintf("c-%d", i), plan)
		}(i)
	}
	wg.Wait()
	ok := 0
	for _, err := range errs {
		if err == nil {
			ok++
		} else if !errors.Is(err, ErrCapacity) {
			t.Fatalf("unexpected: %v", err)
		}
	}
	if ok != 1 {
		t.Fatalf("%d concurrent reservations succeeded, want exactly 1", ok)
	}
	if err := s.ReleaseClusterReservations(ctx, "c-a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Reserve(ctx, "op-b", "cell1", "c-b", plan); err != nil {
		t.Fatalf("released capacity not reusable: %v", err)
	}
}

func TestPublishDesiredNoopOnSameHash(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	op, _, _ := s.CreateCluster(ctx, newCreate("p1", "orders", "k1", "h1"))
	g1, err := s.PublishDesired(ctx, op.ClusterID, "cell1", []byte(`{"a":1}`), "hash1", false)
	if err != nil {
		t.Fatal(err)
	}
	g2, _ := s.PublishDesired(ctx, op.ClusterID, "cell1", []byte(`{"a":1}`), "hash1", false)
	if g1 != g2 {
		t.Fatalf("republishing the same payload bumped the generation %d -> %d", g1, g2)
	}
	g3, _ := s.PublishDesired(ctx, op.ClusterID, "cell1", []byte(`{}`), "hash2", true)
	if g3 <= g2 {
		t.Fatalf("tombstone must bump generation: %d -> %d", g2, g3)
	}
	rows, gen, err := s.DesiredForCell(ctx, "cell1")
	if err != nil || len(rows) != 1 || !rows[0].Tombstone || gen != g3 {
		t.Fatalf("rows=%v gen=%d err=%v", rows, gen, err)
	}
}
