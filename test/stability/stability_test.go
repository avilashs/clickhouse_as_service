//go:build stability

package stability

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avilashs/clickhouse_as_service/internal/engine/clickhouse"
)

// ---------------------------------------------------------------- the controls are in force

// TestControlsLive: what the render promised is what the live server runs (the server booted with
// it, so none of it was rejected), and the platform parts of it are out of the tenant's reach.
func TestControlsLive(t *testing.T) {
	e := setup(t)
	a := e.admin(e.a)

	t.Run("server settings", func(t *testing.T) {
		want := map[string]string{
			"background_pool_size": "2", "background_fetches_pool_size": "2", "background_schedule_pool_size": "32",
			"concurrent_threads_soft_limit_num": "4", "max_table_num_to_throw": "500", "max_database_num_to_throw": "100",
			"max_table_size_to_drop": "0", "max_partition_size_to_drop": "0",
		}
		names := make([]string, 0, len(want))
		for k := range want {
			names = append(names, "'"+k+"'")
		}
		out := must(t, a.q("SELECT name, value FROM system.server_settings WHERE name IN ("+strings.Join(names, ",")+")"))
		got := map[string]string{}
		for _, l := range strings.Split(out, "\n") {
			if f := strings.Split(l, "\t"); len(f) == 2 {
				got[f[0]] = f[1]
			}
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s = %q, want %q", k, got[k], v)
			}
		}
	})

	t.Run("MergeTree thresholds sit below the merge pool", func(t *testing.T) {
		out := must(t, a.q("SELECT name, value FROM system.merge_tree_settings WHERE name LIKE 'number_of_free_entries%' OR name LIKE 'parts_to_%' ORDER BY name"))
		t.Log("\n" + out)
		for _, want := range []string{"parts_to_delay_insert\t1000", "parts_to_throw_insert\t3000",
			"number_of_free_entries_in_pool_to_execute_mutation\t2"} {
			if !strings.Contains(out, want) {
				t.Errorf("missing %q", want)
			}
		}
	})

	t.Run("every system log table has a TTL", func(t *testing.T) {
		// most log tables are created on their first flush
		must(t, a.q("SYSTEM FLUSH LOGS"))
		n := intOf(t, must(t, a.q(`SELECT count() FROM system.tables WHERE database = 'system' AND name LIKE '%\_log' AND name != 'user_query_log'`)))
		bare := must(t, a.q(`SELECT name FROM system.tables WHERE database = 'system' AND name LIKE '%\_log' AND name != 'user_query_log' AND engine_full NOT LIKE '%TTL%'`))
		if n < 18 {
			t.Errorf("only %d log tables exist", n)
		}
		if bare != "" {
			t.Errorf("log tables without a TTL:\n%s", bare)
		}
		if qt := must(t, a.q(`SELECT count() FROM system.tables WHERE database = 'system' AND name = 'query_thread_log'`)); qt != "0" {
			t.Error("query_thread_log should be removed")
		}
		t.Logf("%d log tables, all with a TTL", n)
	})

	t.Run("init containers ran and the balloon exists", func(t *testing.T) {
		for r := 0; r < 2; r++ {
			if s := e.a.status(r); s.Init != "done" || !s.Ready {
				t.Errorf("replica %d: %+v", r, s)
			}
			out, err := kubectl("-n", e.a.ns(), "exec", e.a.pod(r), "-c", "clickhouse", "--", "stat", "-c", "%s", clickhouse.BalloonPath)
			want := fmt.Sprint(clickhouse.DiskReserveBytes(5, true))
			if err != nil || strings.TrimSpace(out) != want {
				t.Errorf("replica %d balloon: %q (%v), want %s bytes", r, strings.TrimSpace(out), err, want)
			}
		}
		keep := must(t, a.q("SELECT keep_free_space FROM system.disks WHERE name = 'default'"))
		if keep != fmt.Sprint(clickhouse.DiskReserveBytes(5, true)) {
			t.Errorf("keep_free_space = %s", keep)
		}
	})

	t.Run("the platform quota exists and tenants cannot touch it", func(t *testing.T) {
		out := must(t, a.q("SHOW CREATE QUOTA "+clickhouse.PlatformQuota))
		if !strings.Contains(out, "TO ALL EXCEPT") || strings.Contains(out, "KEYED BY") {
			t.Errorf("unexpected quota: %s", out)
		}
		for _, sql := range []string{
			"CREATE QUOTA stab_q FOR INTERVAL 1 hour MAX queries = 5",
			"ALTER QUOTA " + clickhouse.PlatformQuota + " FOR INTERVAL 1 hour MAX queries = 999999999",
			"DROP QUOTA " + clickhouse.PlatformQuota,
		} {
			if r := a.q(sql); r.code != 497 {
				t.Errorf("%s: %s, want ACCESS_DENIED (497)", sql, r)
			}
		}
		if !must2(a.q("SHOW CREATE QUOTA " + clickhouse.PlatformQuota)).ok() {
			t.Error("the quota disappeared")
		}
	})
}

func must2(r result) result { return r }

// TestQuotaIsEnforced: a quota stops a tenant-created user once the limit is reached. The
// platform's own quota is shared and set high, so this uses a small one added by the provider
// for one user. Quota usage is counted per server, so with two replicas a user may make up to
// twice the limit. Queries that read nothing (SELECT 1) are not counted by ClickHouse at all.
func TestQuotaIsEnforced(t *testing.T) {
	e := setup(t)
	a, prov := e.admin(e.a), e.provider(t, e.a)
	must(t, a.q("CREATE USER IF NOT EXISTS stab_u IDENTIFIED WITH sha256_password BY 'Stab-u-pw-1'"))
	must(t, a.q("GRANT SELECT ON default.* TO stab_u"))
	must(t, prov.q("CREATE QUOTA OR REPLACE stab_q FOR INTERVAL 1 hour MAX queries = 8 TO stab_u"))
	t.Cleanup(func() {
		prov.q("DROP QUOTA IF EXISTS stab_q")
		a.q("DROP USER IF EXISTS stab_u")
	})
	u := e.connect(e.a, "stab_u", "Stab-u-pw-1", nil)
	var okN, denied int
	for i := 0; i < 40; i++ {
		// a query that reads data: ClickHouse does not count a trivial SELECT 1 against `queries`
		switch r := u.q("SELECT count() FROM numbers(1000)"); {
		case r.ok():
			okN++
		case r.code == 201:
			denied++
		default:
			t.Fatalf("unexpected: %s", r)
		}
	}
	t.Logf("40 queries: %d allowed, %d refused with QUOTA_EXCEEDED", okN, denied)
	if denied == 0 {
		t.Fatal("the quota never stopped the user")
	}
	if okN < 1 || okN > 16 {
		t.Errorf("%d queries allowed with a limit of 8 per server on 2 servers", okN)
	}
}

// ---------------------------------------------------------------- small inserts

// TestAsyncInsertAcknowledgesAfterTheFlush: with wait_for_async_insert pinned, the client gets its
// answer only once the data is written, so an acknowledged insert is a durable one, and it
// cannot be turned off.
func TestAsyncInsertAcknowledgesAfterTheFlush(t *testing.T) {
	e := setup(t)
	a := e.admin(e.a)
	must(t, a.q("CREATE TABLE IF NOT EXISTS default.stab_async ON CLUSTER default (x UInt64) ENGINE = ReplicatedMergeTree ORDER BY x"))
	t.Cleanup(func() { a.q("DROP TABLE IF EXISTS default.stab_async ON CLUSTER default SYNC") })
	r := a.qp("INSERT INTO default.stab_async VALUES (1)", url.Values{
		"async_insert": {"1"}, "wait_for_async_insert": {"1"},
		"async_insert_use_adaptive_busy_timeout": {"0"}, "async_insert_busy_timeout_max_ms": {"3000"},
	})
	if !r.ok() {
		t.Fatalf("insert: %s", r)
	}
	t.Logf("acknowledged after %v (flush timeout 3s)", r.dur.Round(10*time.Millisecond))
	if r.dur < 2500*time.Millisecond {
		t.Errorf("acknowledged after %v: before the flush", r.dur)
	}
	if got := must(t, a.q("SELECT count() FROM default.stab_async")); got != "1" {
		t.Errorf("the acknowledged row is not there: count = %s", got)
	}
	if r := a.qp("INSERT INTO default.stab_async VALUES (2)", url.Values{"async_insert": {"1"}, "wait_for_async_insert": {"0"}}); r.code != 452 {
		t.Errorf("wait_for_async_insert = 0 must be refused (452), got %s", r)
	}
}

// ---------------------------------------------------------------- memory (F-8)

const hog = "SELECT length(groupArray(toString(number))) FROM numbers(2000000000)"

// TestRunawayQueriesDoNotKillThePod: queries that would use far more memory than the pod has fail
// with MEMORY_LIMIT_EXCEEDED, one at a time and eight at once (their sum exceeds the server's
// limit), and the containers neither restart nor get OOM-killed.
func TestRunawayQueriesDoNotKillThePod(t *testing.T) {
	e := setup(t)
	before := [2]podStatus{e.a.status(0), e.a.status(1)}
	t.Run("one runaway query", func(t *testing.T) {
		if r := e.admin(e.a).q(hog); r.code != 241 {
			t.Fatalf("want MEMORY_LIMIT_EXCEEDED (241), got %s", r)
		}
	})
	t.Run("eight at once", func(t *testing.T) {
		var wg sync.WaitGroup
		res := make([]result, 8)
		for i := range res {
			wg.Add(1)
			go func(i int) { defer wg.Done(); res[i] = e.admin(e.a).q(hog) }(i)
		}
		wg.Wait()
		codes := map[string]int{}
		for _, r := range res {
			switch {
			case r.err != "":
				t.Errorf("connection lost (pod killed?): %s", r)
				codes["transport error"]++
			case r.code == 241:
				codes["241 MEMORY_LIMIT_EXCEEDED"]++
			default:
				t.Errorf("unexpected: %s", r)
				codes[fmt.Sprint(r.code)]++
			}
		}
		t.Logf("results: %v", codes)
	})
	time.Sleep(3 * time.Second)
	for r := 0; r < 2; r++ {
		after := e.a.status(r)
		if after.Restarts != before[r].Restarts || after.OOM || !after.Ready {
			t.Errorf("replica %d: before %+v after %+v", r, before[r], after)
		}
		if out, err := e.a.podSQL(r, "SELECT 1"); err != nil || out != "1" {
			t.Errorf("replica %d does not answer: %q %v", r, out, err)
		}
	}
}

// ---------------------------------------------------------------- too many parts (F-7)

// TestInsertStormIsContainedAndNeighbourUnaffected: with merges stopped, tiny synchronous inserts
// pile up parts until inserts are slowed (1000 parts) and then refused (3000). A neighbour on the
// same shared nodes keeps answering. Afterwards merges catch up and inserts work again.
func TestInsertStormIsContainedAndNeighbourUnaffected(t *testing.T) {
	e := setup(t)
	a, b := e.admin(e.a), e.admin(e.b)
	must(t, a.q("CREATE TABLE IF NOT EXISTS default.stab_storm ON CLUSTER default (id UInt64, v String) ENGINE = ReplicatedMergeTree ORDER BY id"))
	t.Cleanup(func() {
		a.q("SYSTEM START MERGES ON CLUSTER default default.stab_storm")
		a.q("DROP TABLE IF EXISTS default.stab_storm ON CLUSTER default SYNC")
	})
	must(t, a.q("SYSTEM STOP MERGES ON CLUSTER default default.stab_storm"))

	const probeSQL = "SELECT count() FROM numbers(3000000)"
	runProbe := func(d time.Duration) (latencies, []result) {
		stop := make(chan struct{})
		time.AfterFunc(d, func() { close(stop) })
		return probe(b, probeSQL, 200*time.Millisecond, stop)
	}
	base, baseFails := runProbe(20 * time.Second)
	t.Logf("neighbour baseline: n=%d p50=%v p99=%v failures=%d", len(base), base.pct(.5), base.pct(.99), len(baseFails))
	if len(base) < 20 || len(baseFails) > 0 {
		t.Fatalf("neighbour probe unusable before the storm: %d ok, %d failed", len(base), len(baseFails))
	}

	// the storm
	var stormLat latencies
	var stormFails []result
	probeDone := make(chan struct{})
	stopProbe := make(chan struct{})
	go func() { stormLat, stormFails = probe(b, probeSQL, 200*time.Millisecond, stopProbe); close(probeDone) }()

	var inserted, tooMany, other, delayedSeen atomic.Int64
	var maxParts atomic.Int64
	var slowest atomic.Int64
	deadline := time.Now().Add(7 * time.Minute)
	var wg sync.WaitGroup
	stopStorm := make(chan struct{})
	var once sync.Once
	for w := 0; w < 24; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stopStorm:
					return
				default:
				}
				if time.Now().After(deadline) || inserted.Load() >= 5000 {
					once.Do(func() { close(stopStorm) })
					return
				}
				r := a.qp(fmt.Sprintf("INSERT INTO default.stab_storm VALUES (%d, 'x')", w*1000000+i), url.Values{"async_insert": {"0"}})
				switch {
				case r.ok():
					inserted.Add(1)
					if d := r.dur.Milliseconds(); d > slowest.Load() {
						slowest.Store(d)
					}
				case r.code == 252: // TOO_MANY_PARTS
					if tooMany.Add(1) >= 20 {
						once.Do(func() { close(stopStorm) })
					}
				default:
					if other.Add(1) <= 3 {
						t.Logf("insert: %s", r)
					}
				}
			}
		}(w)
	}
	// watch the part count and the delay counter while the storm runs
	watch := time.NewTicker(3 * time.Second)
	go func() {
		for range watch.C {
			if r := a.q("SELECT max(value) FROM system.asynchronous_metrics WHERE metric = 'MaxPartCountForPartition'"); r.ok() {
				if n := int64(intOf(t, strings.Split(strings.TrimSpace(r.body), ".")[0])); n > maxParts.Load() {
					maxParts.Store(n)
				}
			}
			if r := a.q("SELECT value FROM system.metrics WHERE metric = 'DelayedInserts'"); r.ok() && strings.TrimSpace(r.body) != "0" {
				delayedSeen.Add(1)
			}
		}
	}()
	wg.Wait()
	watch.Stop()
	close(stopProbe)
	<-probeDone
	t.Logf("storm: %d inserts ok, %d TOO_MANY_PARTS, %d other errors, slowest insert %dms, max parts in a partition seen %d, DelayedInserts>0 in %d samples",
		inserted.Load(), tooMany.Load(), other.Load(), slowest.Load(), maxParts.Load(), delayedSeen.Load())
	t.Logf("neighbour during storm: n=%d p50=%v p99=%v failures=%d", len(stormLat), stormLat.pct(.5), stormLat.pct(.99), len(stormFails))

	// the design allows either outcome: slowed at parts_to_delay_insert, or refused at parts_to_throw_insert
	if tooMany.Load() == 0 && delayedSeen.Load() == 0 {
		t.Error("inserts were neither slowed down (DelayedInserts) nor refused (TOO_MANY_PARTS)")
	}
	if tooMany.Load() == 0 {
		t.Logf("note: no insert was refused; the parts of one partition were spread over two replicas and peaked at %d", maxParts.Load())
	}
	if len(stormFails) > 0 {
		t.Errorf("the neighbour had %d failed queries, e.g. %s", len(stormFails), stormFails[0])
	}
	if limit := 3*base.pct(.99) + 500*time.Millisecond; stormLat.pct(.99) > limit {
		t.Errorf("neighbour p99 %v during the storm exceeds %v (3 x baseline p99 + 500ms)", stormLat.pct(.99), limit)
	}

	// recovery: merges must make steady progress and inserts must keep working. How long a full
	// recovery takes depends on the server's size and the load: on this 4-vCPU VM, a 1-vCPU
	// server under gVisor merged 4559 parts down to 1333 in 13 minutes in one run, and 5023
	// down to 1760 in 5 minutes in another, so no wall-clock target is asserted.
	must(t, a.q("SYSTEM START MERGES ON CLUSTER default default.stab_storm"))
	peak := intOf(t, must(t, a.q("SELECT count() FROM system.parts WHERE table = 'stab_storm' AND active")))
	start := time.Now()
	parts := peak
	for i := 0; i < 120 && parts > peak/2; i++ { // up to 10 minutes
		time.Sleep(5 * time.Second)
		parts = intOf(t, must(t, a.q("SELECT count() FROM system.parts WHERE table = 'stab_storm' AND active")))
	}
	el := time.Since(start)
	t.Logf("active parts: %d at the start of recovery, %d after %v (%.0f parts/min)", peak, parts, el.Round(time.Second), float64(peak-parts)/el.Minutes())
	if parts > peak/2 {
		t.Errorf("merges did not halve the part count in 10 minutes: %d -> %d", peak, parts)
	}
	if r := a.qp("INSERT INTO default.stab_storm VALUES (1, 'after')", url.Values{"async_insert": {"0"}}); !r.ok() {
		t.Errorf("inserts do not work after recovery: %s", r)
	}
}

// ---------------------------------------------------------------- object counts

func TestObjectCountLimits(t *testing.T) {
	e := setup(t)
	a := e.admin(e.a) // one keep-alive connection: the same replica for every statement
	t.Run("databases", func(t *testing.T) {
		n := 0
		var last result
		for ; n < 400; n++ {
			if last = a.q(fmt.Sprintf("CREATE DATABASE stab_db_%d", n)); !last.ok() {
				break
			}
		}
		t.Cleanup(func() {
			for i := 0; i <= n; i++ {
				a.q(fmt.Sprintf("DROP DATABASE IF EXISTS stab_db_%d SYNC", i))
			}
		})
		t.Logf("created %d databases, then: %s", n, last)
		if last.ok() || !strings.Contains(last.body, "max_database_num_to_throw") {
			t.Fatalf("expected the limit to stop it: %s", last)
		}
		if n < 50 || n > 100 {
			t.Errorf("stopped after %d databases with a limit of 100", n)
		}
	})
	t.Run("tables", func(t *testing.T) {
		must(t, a.q("CREATE DATABASE IF NOT EXISTS stab_t"))
		t.Cleanup(func() { a.q("DROP DATABASE IF EXISTS stab_t SYNC") })
		n := 0
		var last result
		for ; n < 700; n++ {
			if last = a.q(fmt.Sprintf("CREATE TABLE stab_t.t_%d (x UInt8) ENGINE = Memory", n)); !last.ok() {
				break
			}
		}
		t.Logf("created %d tables, then: %s", n, last)
		if last.ok() || !strings.Contains(last.body, "max_table_num_to_throw") {
			t.Fatalf("expected the limit to stop it: %s", last)
		}
		if n < 400 || n > 500 {
			t.Errorf("stopped after %d tables with a limit of 500", n)
		}
	})
}

// ---------------------------------------------------------------- Keeper quorum loss (F-5)

func (c cluster) keeperSTS(i int) string { return fmt.Sprintf("chk-%s-keeper-0-%d", c.id, i) }

func (c cluster) scaleKeeper(t *testing.T, i, n int) {
	t.Helper()
	if out, err := kubectl("-n", c.ns(), "scale", "statefulset", c.keeperSTS(i), fmt.Sprintf("--replicas=%d", n)); err != nil {
		t.Fatalf("scale keeper %d to %d: %s", i, n, out)
	}
}

func (c cluster) keepersReady() int {
	n := 0
	for i := 0; i < 3; i++ {
		out, _ := kubectl("-n", c.ns(), "get", "pod", c.keeperSTS(i)+"-0", "-o", "jsonpath={.status.containerStatuses[0].ready}")
		if strings.TrimSpace(out) == "true" {
			n++
		}
	}
	return n
}

func TestKeeperQuorumLoss(t *testing.T) {
	e := setup(t)
	c, a := e.a, e.admin(e.a)
	must(t, a.q("CREATE TABLE IF NOT EXISTS default.stab_keeper ON CLUSTER default (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"))
	t.Cleanup(func() { a.q("DROP TABLE IF EXISTS default.stab_keeper ON CLUSTER default SYNC") })
	must(t, a.q("INSERT INTO default.stab_keeper VALUES (1)"))
	for r := 0; r < 2; r++ { // both replicas hold the row before the outage (replication is asynchronous)
		if out, err := c.podSQL(r, "SYSTEM SYNC REPLICA default.stab_keeper"); err != nil {
			t.Fatalf("sync replica %d: %s", r, out)
		}
		if out, _ := c.podSQL(r, "SELECT count() FROM default.stab_keeper"); out != "1" {
			t.Fatalf("replica %d holds %q rows before the outage", r, out)
		}
	}
	clusterState := func() string {
		_, body := e.apiGet("/v1/projects/"+c.project+"/clusters/"+c.id, c.token)
		var d struct{ State, StateReason string }
		json.Unmarshal(body, &d)
		return d.State + " " + d.StateReason
	}
	if st := clusterState(); !strings.HasPrefix(st, "ACTIVE") {
		t.Fatalf("cluster state before the outage: %s", st)
	}

	restored := false
	restore := func() {
		if !restored {
			restored = true
			kubectl("-n", c.ns(), "scale", "statefulset", c.keeperSTS(1), "--replicas=1")
			kubectl("-n", c.ns(), "scale", "statefulset", c.keeperSTS(2), "--replicas=1")
		}
	}
	t.Cleanup(restore)

	// quorum is lost: 2 of 3 Keeper nodes stop
	c.scaleKeeper(t, 1, 0)
	c.scaleKeeper(t, 2, 0)
	var readonly string
	for i := 0; i < 40; i++ {
		readonly, _ = c.podSQL(0, "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'")
		if readonly != "" && readonly != "0" {
			break
		}
		time.Sleep(3 * time.Second)
	}
	t.Logf("ReadonlyReplica on replica 0: %q", readonly)
	if readonly == "" || readonly == "0" {
		t.Fatal("the replica never became read-only")
	}
	t.Run("inserts fail, selects work", func(t *testing.T) {
		// with ClickHouse's default retries (20, with backoff) an insert rides out a short Keeper
		// outage, which is wanted for a Keeper restart; during a real outage it is never acknowledged
		patient := e.admin(c)
		patient.http.Timeout = 20 * time.Second
		if r := patient.q("INSERT INTO default.stab_keeper VALUES (2)"); r.ok() {
			t.Errorf("an insert was acknowledged without Keeper quorum: %s", r)
		} else {
			t.Logf("default retries, 20s client timeout: %s", r)
		}
		// without retries the refusal is immediate and explicit
		r := a.qp("INSERT INTO default.stab_keeper VALUES (3)", url.Values{"async_insert": {"0"}, "insert_keeper_max_retries": {"0"}})
		t.Logf("no retries: %s", r)
		if r.ok() || (r.code != 242 && r.code != 999) {
			t.Errorf("want TABLE_IS_READ_ONLY (242) or KEEPER_EXCEPTION (999), got %s", r)
		}
		for rep := 0; rep < 2; rep++ {
			if out, err := c.podSQL(rep, "SELECT count() FROM default.stab_keeper"); err != nil || out != "1" {
				t.Errorf("select on replica %d: %q %v", rep, out, err)
			}
		}
	})

	t.Run("the API reports DEGRADED", func(t *testing.T) {
		var st string
		for i := 0; i < 36; i++ {
			if st = clusterState(); strings.HasPrefix(st, "DEGRADED") {
				t.Logf("after ~%ds: %s", i*5, st)
				return
			}
			time.Sleep(5 * time.Second)
		}
		t.Errorf("the cluster still reports %q", st)
	})

	t.Run("a replica that restarts waits for Keeper instead of crash-looping", func(t *testing.T) {
		oldUID := c.status(1).UID
		kubectl("-n", c.ns(), "delete", "pod", c.pod(1), "--wait=false")
		var sawWaiting, crashed bool
		var last podStatus
		for i := 0; i < 25; i++ {
			time.Sleep(3 * time.Second)
			if last = c.status(1); last.UID == oldUID {
				continue // the old pod is still terminating
			}
			if last.Init == "wait-for-keeper" {
				sawWaiting = true
			}
			if last.Restarts > 0 {
				crashed = true
			}
		}
		t.Logf("after 75s of outage: %+v", last)
		if !sawWaiting {
			t.Error("the pod was never seen waiting in the wait-for-keeper init container")
		}
		if crashed || last.Ready || last.Init == "done" {
			t.Errorf("the server container started without Keeper: %+v", last)
		}
	})

	// quorum returns
	restore()
	t0 := time.Now()
	var keeperReady, serverReady time.Duration
	for i := 0; i < 120 && serverReady == 0; i++ {
		time.Sleep(3 * time.Second)
		if keeperReady == 0 && c.keepersReady() == 3 {
			keeperReady = time.Since(t0)
		}
		if c.status(1).Ready && keeperReady > 0 {
			serverReady = time.Since(t0)
		}
	}
	t.Logf("after the Keeper nodes were started: all 3 ready after %v, restarted replica ready after %v", keeperReady.Round(time.Second), serverReady.Round(time.Second))
	if serverReady == 0 {
		t.Fatal("the restarted replica did not come back")
	}
	t.Run("the API reports ACTIVE again", func(t *testing.T) {
		var st string
		for i := 0; i < 36; i++ {
			if st = clusterState(); strings.HasPrefix(st, "ACTIVE") {
				return
			}
			time.Sleep(5 * time.Second)
		}
		t.Errorf("the cluster still reports %q", st)
	})
	t.Run("inserts work again", func(t *testing.T) {
		var r result
		for i := 0; i < 30; i++ {
			if r = a.q("INSERT INTO default.stab_keeper VALUES (3)"); r.ok() {
				return
			}
			time.Sleep(3 * time.Second)
		}
		t.Errorf("still failing: %s", r)
	})
}

// ---------------------------------------------------------------- disk full (F-4)

// TestDiskFull puts replica 0's data directory on a small tmpfs (the pod's volume is a directory
// on the VM's 250 GB disk), fills it, and checks the design's behaviour: writes are refused before
// the volume is full, reads keep working, the pod stays up, and deleting the balloon file gives
// the operator headroom. It runs last because it restarts a replica.
func TestDiskFull(t *testing.T) {
	e := setup(t)
	c := e.a
	pod := c.pod(0)
	field := func(args ...string) string {
		out, err := kubectl(args...)
		if err != nil {
			t.Fatalf("%v: %s", args, out)
		}
		return strings.TrimSpace(out)
	}
	node := field("-n", c.ns(), "get", "pod", pod, "-o", "jsonpath={.spec.nodeName}")
	pv := field("-n", c.ns(), "get", "pvc", "data-"+pod, "-o", "jsonpath={.spec.volumeName}")
	dir := field("get", "pv", pv, "-o", "jsonpath={.spec.hostPath.path}")
	if dir == "" {
		dir = field("get", "pv", pv, "-o", "jsonpath={.spec.local.path}")
	}
	sh := func(script string) string {
		out, err := run("docker", "exec", node, "sh", "-c", script)
		if err != nil {
			t.Fatalf("on %s: %s\n%s", node, script, out)
		}
		return strings.TrimSpace(out)
	}
	t.Logf("replica 0 runs on %s, data directory %s", node, dir)
	t.Cleanup(func() { run("docker", "exec", node, "umount", "-l", dir) })

	// freeze the server so nothing is written while the directory moves onto the tmpfs
	if out, err := kubectl("-n", c.ns(), "exec", pod, "-c", "clickhouse", "--", "bash", "-c", "kill -STOP 1"); err != nil {
		t.Fatalf("cannot freeze the server: %s", out)
	}
	usedMB := intOf(t, sh("du -sm "+dir+" | cut -f1"))
	reserveMB := clickhouse.DiskReserveBytes(5, true) / (1 << 20)
	sizeMB := usedMB + 3*reserveMB + 64 // the balloon is part of used; room for ~one reserve of data beyond keep_free_space
	sh(fmt.Sprintf("set -e; mkdir -p /tmp/stabcopy; cp -a %s/. /tmp/stabcopy/; mount -t tmpfs -o size=%dm tmpfs %s; cp -a /tmp/stabcopy/. %s/; rm -rf /tmp/stabcopy", dir, sizeMB, dir, dir))
	t.Logf("data directory on a %d MiB tmpfs (%d MiB in use, reserve %d MiB)", sizeMB, usedMB, reserveMB)
	kubectl("-n", c.ns(), "delete", "pod", pod, "--grace-period=1", "--wait=false")
	for i := 0; i < 60; i++ {
		time.Sleep(3 * time.Second)
		if s := c.status(0); s.Ready && s.Restarts == 0 {
			break
		}
	}
	if !c.status(0).Ready {
		t.Fatalf("replica 0 did not come back on the tmpfs: %+v", c.status(0))
	}
	restartsBefore := c.status(0).Restarts

	disk := func() string {
		out, _ := c.podSQL(0, "SELECT free_space, keep_free_space FROM system.disks WHERE name = 'default' FORMAT TSV")
		return out
	}
	t.Logf("disk as the server sees it (free_space, keep_free_space): %s", disk())
	if _, err := c.podSQL(0, "CREATE TABLE IF NOT EXISTS default.stab_fill ON CLUSTER default (id UInt64, s String) ENGINE = ReplicatedMergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.podSQL(0, "DROP TABLE IF EXISTS default.stab_fill ON CLUSTER default SYNC") })
	insert := "INSERT INTO default.stab_fill SETTINGS async_insert = 0 SELECT rand64(), randomPrintableASCII(1000) FROM numbers(20000)"

	// fill until a write is refused
	var refusal string
	inserted := 0
	for i := 0; i < 40; i++ {
		out, err := c.podSQL(0, insert)
		if err != nil {
			refusal = out
			break
		}
		inserted++
	}
	t.Logf("%d inserts of ~20 MB succeeded; then: %.300s", inserted, strings.ReplaceAll(refusal, "\n", " "))
	t.Logf("disk at the refusal (free_space, keep_free_space): %s", disk())
	if refusal == "" {
		t.Fatal("the volume never filled up")
	}
	if !strings.Contains(refusal, "NOT_ENOUGH_SPACE") && !strings.Contains(refusal, "Code: 243") {
		t.Errorf("expected NOT_ENOUGH_SPACE (243), got: %.300s", refusal)
	}
	t.Run("stopped before the volume was full", func(t *testing.T) {
		enospc, _ := kubectl("-n", c.ns(), "exec", pod, "-c", "clickhouse", "--", "bash", "-c",
			"grep -c 'No space left on device' /var/log/clickhouse-server/clickhouse-server.err.log || true")
		if strings.TrimSpace(enospc) != "0" {
			t.Errorf("%s ENOSPC errors in the server log: writes hit the end of the volume", strings.TrimSpace(enospc))
		}
	})
	t.Run("reads still work and the pod stayed up", func(t *testing.T) {
		out, err := c.podSQL(0, "SELECT count() > 0 FROM default.stab_fill")
		if err != nil || out != "1" {
			t.Errorf("select: %q %v", out, err)
		}
		if s := c.status(0); s.Restarts != restartsBefore || !s.Ready {
			t.Errorf("pod: %+v (restarts before %d)", s, restartsBefore)
		}
	})
	t.Run("deleting the balloon gives headroom", func(t *testing.T) {
		before := disk()
		if out, err := kubectl("-n", c.ns(), "exec", pod, "-c", "clickhouse", "--", "rm", clickhouse.BalloonPath); err != nil {
			t.Fatalf("rm balloon: %s", out)
		}
		t.Logf("free_space, keep_free_space before the release: %s; after: %s", before, disk())
		if out, err := c.podSQL(0, insert); err != nil {
			t.Errorf("an insert still fails after the balloon was released: %.300s", out)
		}
	})
	t.Run("dropping data frees the volume for good", func(t *testing.T) {
		if out, err := c.podSQL(0, "DROP TABLE default.stab_fill ON CLUSTER default SYNC"); err != nil {
			t.Fatalf("drop: %s", out)
		}
		if out, err := c.podSQL(0, "CREATE TABLE default.stab_fill2 (id UInt64, s String) ENGINE = Memory"); err != nil {
			t.Fatalf("create: %s", out)
		}
		c.podSQL(0, "DROP TABLE default.stab_fill2")
		t.Logf("disk after the drop: %s", disk())
	})
}
