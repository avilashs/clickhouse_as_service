//go:build lifecycle

package lifecycle

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Verifier is the durability check of design §13.6. Writers insert batches of rows with
// quorum inserts and keep a ledger of the batches the server ACKNOWLEDGED; readers run a query
// continuously. After the change under test, every acknowledged batch must be present, complete
// and not duplicated on every replica.
//
// The guarantee being tested depends on the client using insert_quorum = 'auto' (a majority of
// the replicas must have the part before the insert is acknowledged), with insert_quorum_parallel
// = 1 and a quorum timeout longer than a replica restart. With ClickHouse's default
// insert_quorum = 0 an insert acknowledged by one replica can be lost if that replica's disk is
// lost before the others fetch the part; a graceful rolling restart does not lose disks, which is
// why Quorum "0" is also supported here, as a comparison.
type Verifier struct {
	c         *client
	cl        cluster
	Quorum    string // insert_quorum value
	BatchRows int
	Writers   int
	Readers   int
	Pause     time.Duration // between a writer's batches

	next atomic.Int64
	stop chan struct{}
	wg   sync.WaitGroup

	mu        sync.Mutex
	acked     map[int64]time.Time // batch -> ack time
	attempted map[int64]bool
	attempts  int
	failed    []failure
	reads     []readSample
}

type failure struct {
	at   time.Time
	code int
	msg  string
}

type readSample struct {
	at   time.Time
	dur  time.Duration
	ok   bool
	torn bool // count is not a multiple of BatchRows: a part was visible partially
	code int
}

const verifyTable = "default.life_verify"

func newVerifier(e *env, quorum string) *Verifier {
	return &Verifier{c: e.admin(e.a), cl: e.a, Quorum: quorum, BatchRows: 2000, Writers: 3, Readers: 2, Pause: 150 * time.Millisecond,
		stop: make(chan struct{}), acked: map[int64]time.Time{}, attempted: map[int64]bool{}}
}

func (v *Verifier) Setup(t *testing.T) {
	t.Helper()
	must(t, v.c.q("DROP TABLE IF EXISTS "+verifyTable+" ON CLUSTER default SYNC"))
	must(t, v.c.q("CREATE TABLE "+verifyTable+" ON CLUSTER default (batch UInt64, i UInt32, payload String) ENGINE = ReplicatedMergeTree ORDER BY (batch, i)"))
}

func (v *Verifier) batchBody(id int64) string {
	var b strings.Builder
	b.WriteString("INSERT INTO " + verifyTable + " FORMAT TSV\n")
	for i := 0; i < v.BatchRows; i++ {
		fmt.Fprintf(&b, "%d\t%d\tp%d-%d\n", id, i, id, i)
	}
	return b.String()
}

func (v *Verifier) stopped() bool {
	select {
	case <-v.stop:
		return true
	default:
		return false
	}
}

// Start launches the writers and readers.
func (v *Verifier) Start() {
	params := url.Values{
		"insert_quorum": {v.Quorum}, "insert_quorum_parallel": {"1"}, "insert_quorum_timeout": {"120000"},
		"async_insert": {"0"},
	}
	for w := 0; w < v.Writers; w++ {
		v.wg.Add(1)
		go func() {
			defer v.wg.Done()
			for !v.stopped() {
				id := v.next.Add(1)
				body := v.batchBody(id)
				v.mu.Lock()
				v.attempted[id] = true
				v.mu.Unlock()
				// retry the identical block until it is acknowledged: the table deduplicates it
				for backoff := 200 * time.Millisecond; ; {
					r := v.c.qp(body, params)
					v.mu.Lock()
					v.attempts++
					if r.ok() {
						v.acked[id] = time.Now()
						v.mu.Unlock()
						break
					}
					v.failed = append(v.failed, failure{time.Now(), r.code, strings.TrimSpace(r.err + " " + firstLine(r.body))})
					v.mu.Unlock()
					if v.stopped() {
						break
					}
					time.Sleep(backoff)
					if backoff < 3*time.Second {
						backoff *= 2
					}
				}
				time.Sleep(v.Pause)
			}
		}()
	}
	for r := 0; r < v.Readers; r++ {
		v.wg.Add(1)
		go func() {
			defer v.wg.Done()
			for !v.stopped() {
				res := v.c.q("SELECT count(), uniqExact(batch) FROM " + verifyTable)
				s := readSample{at: time.Now(), dur: res.dur, ok: res.ok(), code: res.code}
				if res.ok() {
					f := strings.Fields(res.body)
					if len(f) == 2 {
						n, _ := strconv.ParseInt(f[0], 10, 64)
						s.torn = n%int64(v.BatchRows) != 0
					}
				}
				v.mu.Lock()
				v.reads = append(v.reads, s)
				v.mu.Unlock()
				time.Sleep(250 * time.Millisecond)
			}
		}()
	}
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 160 {
		s = s[:160]
	}
	return s
}

// Stop ends the load and waits for the workers.
func (v *Verifier) Stop() {
	close(v.stop)
	v.wg.Wait()
}

// Window summarises what happened between two instants.
type Window struct {
	Acked, FailedAttempts, Reads, ReadFailures, TornReads int
	LongestWriteGap                                       time.Duration // longest time with no acknowledgement
	ReadP50, ReadP99                                      time.Duration
	Codes                                                 map[int]int
}

func (w Window) String() string {
	return fmt.Sprintf("%d batches acked, %d failed attempts %v, longest gap between acks %v; %d reads (%d failed, %d torn), p50 %v p99 %v",
		w.Acked, w.FailedAttempts, w.Codes, w.LongestWriteGap.Round(100*time.Millisecond), w.Reads, w.ReadFailures, w.TornReads,
		w.ReadP50.Round(time.Millisecond), w.ReadP99.Round(time.Millisecond))
}

func (v *Verifier) Between(from, to time.Time) Window {
	v.mu.Lock()
	defer v.mu.Unlock()
	w := Window{Codes: map[int]int{}}
	var acks []time.Time
	for _, at := range v.acked {
		if !at.Before(from) && at.Before(to) {
			w.Acked++
			acks = append(acks, at)
		}
	}
	sort.Slice(acks, func(i, j int) bool { return acks[i].Before(acks[j]) })
	prev := from
	for _, a := range acks {
		if g := a.Sub(prev); g > w.LongestWriteGap {
			w.LongestWriteGap = g
		}
		prev = a
	}
	if g := to.Sub(prev); g > w.LongestWriteGap {
		w.LongestWriteGap = g
	}
	for _, f := range v.failed {
		if !f.at.Before(from) && f.at.Before(to) {
			w.FailedAttempts++
			w.Codes[f.code]++
		}
	}
	var lat latencies
	for _, r := range v.reads {
		if r.at.Before(from) || !r.at.Before(to) {
			continue
		}
		w.Reads++
		if !r.ok {
			w.ReadFailures++
			continue
		}
		if r.torn {
			w.TornReads++
		}
		lat = append(lat, r.dur)
	}
	w.ReadP50, w.ReadP99 = lat.pct(.5), lat.pct(.99)
	return w
}

// Result of checking one replica against the ledger.
type ReplicaCheck struct {
	Replica                               int
	Batches                               int
	Missing, Partial, Duplicated, Unacked int
	MissingIDs                            []int64
}

// Verify waits for every replica to catch up and compares what it holds with the ledger.
func (v *Verifier) Verify(t *testing.T, replicas int) []ReplicaCheck {
	t.Helper()
	v.mu.Lock()
	acked := make(map[int64]bool, len(v.acked))
	for id := range v.acked {
		acked[id] = true
	}
	attempted := len(v.attempted)
	v.mu.Unlock()
	t.Logf("ledger: %d batches acknowledged out of %d attempted, %d rows per batch", len(acked), attempted, v.BatchRows)
	var out []ReplicaCheck
	for r := 0; r < replicas; r++ {
		var lastErr string
		synced := false
		for i := 0; i < 30 && !synced; i++ {
			o, err := v.cl.podSQL(r, "SYSTEM SYNC REPLICA "+verifyTable)
			if err == nil {
				synced = true
				break
			}
			lastErr = o
			time.Sleep(10 * time.Second)
		}
		if !synced {
			t.Fatalf("replica %d did not sync: %s", r, lastErr)
		}
		out2, err := v.cl.podSQL(r, "SELECT batch, count() FROM "+verifyTable+" GROUP BY batch ORDER BY batch FORMAT TSV")
		if err != nil {
			t.Fatalf("replica %d: %s", r, out2)
		}
		have := map[int64]int{}
		for _, l := range strings.Split(out2, "\n") {
			f := strings.Fields(l)
			if len(f) != 2 {
				continue
			}
			id, _ := strconv.ParseInt(f[0], 10, 64)
			n, _ := strconv.Atoi(f[1])
			have[id] = n
		}
		rc := ReplicaCheck{Replica: r, Batches: len(have)}
		for id := range acked {
			n, ok := have[id]
			switch {
			case !ok:
				rc.Missing++
				if len(rc.MissingIDs) < 10 {
					rc.MissingIDs = append(rc.MissingIDs, id)
				}
			case n < v.BatchRows:
				rc.Partial++
			case n > v.BatchRows:
				rc.Duplicated++
			}
		}
		for id, n := range have {
			if !acked[id] {
				rc.Unacked++
				if n > v.BatchRows {
					rc.Duplicated++
				}
			}
		}
		out = append(out, rc)
	}
	return out
}
