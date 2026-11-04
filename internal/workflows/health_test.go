package workflows

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/engine/clickhouse"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

func TestSyncHealthFollowsTheAgentsObservations(t *testing.T) {
	s := testStore(t, 8000)
	c, op := seedCluster(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	fakeAgent(ctx, t, s, true)
	e := env(t, s)
	e.ExecuteWorkflow(CreateCluster, Input{OperationID: op.ID, ClusterID: c.ID})
	if e.GetWorkflowError() != nil {
		t.Fatalf("create: %v", e.GetWorkflowError())
	}
	cancel() // from here the test writes the observations
	engines := map[string]engine.Engine{"clickhouse": clickhouse.Engine{}}
	bg := context.Background()
	log := slog.Default()
	state := func() (model.ClusterState, string) {
		got, _ := s.GetCluster(bg, "", c.ID)
		return got.State, got.StateReason
	}
	observe := func(keeperOK bool, at time.Time) {
		row, _ := s.GetDesired(bg, c.ID)
		o := engine.Observed{ClusterID: c.ID, AppliedGeneration: row.Generation, AppliedHash: row.Hash, BootstrapHash: row.Hash,
			NamespaceExists: true, ObservedAt: at, PodsReady: 5, PodsTotal: 5,
			Objects: map[string]string{"ClickHouseKeeperInstallation/" + c.ID: "Completed", "ClickHouseInstallation/" + c.ID: "Completed"},
			Probes:  []engine.HostProbe{{Host: "a", OK: true, ClusterHosts: 2, KeeperOK: keeperOK}, {Host: "b", OK: true, ClusterHosts: 2, KeeperOK: true}}}
		if err := s.RecordObserved(bg, "cell1", []engine.Observed{o}); err != nil {
			t.Fatal(err)
		}
	}
	sync := func(now time.Time) int {
		n, err := SyncHealth(bg, s, engines, now, log)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	now := time.Now()
	observe(true, now)
	if n := sync(now); n != 0 {
		t.Fatalf("a healthy ACTIVE cluster changed (%d)", n)
	}
	observe(false, now) // one replica lost its Keeper session
	if n := sync(now); n != 1 {
		t.Fatalf("changed %d", n)
	}
	if st, reason := state(); st != model.ClusterDegraded || reason == "" {
		t.Fatalf("state %s (%q), want DEGRADED with a reason", st, reason)
	}
	if n := sync(now); n != 0 {
		t.Fatal("an unchanged DEGRADED cluster was written again")
	}
	observe(true, now) // Keeper is back
	sync(now)
	if st, reason := state(); st != model.ClusterActive || reason != "" {
		t.Fatalf("state %s (%q), want ACTIVE", st, reason)
	}

	// an agent that stopped reporting says nothing: the state stays
	observe(false, now.Add(-10*time.Minute))
	if n := sync(now); n != 0 {
		t.Fatal("a stale observation changed the state")
	}

	// a workflow-owned state is never overwritten
	if err := s.SetClusterState(bg, c.ID, model.ClusterDeleting, ""); err != nil {
		t.Fatal(err)
	}
	observe(false, now)
	if n := sync(now); n != 0 {
		t.Fatal("health sync touched a DELETING cluster")
	}
	if st, _ := state(); st != model.ClusterDeleting {
		t.Fatalf("state %s", st)
	}
}
