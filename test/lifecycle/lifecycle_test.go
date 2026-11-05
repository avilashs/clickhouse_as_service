//go:build lifecycle

package lifecycle

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

type step struct {
	name, body string
	replicas   int
	size       string
	version    string // "" keeps the version
}

var allSteps = map[string]step{
	"add-replica": {name: "add replica (t-2 -> t-3r)", body: `{"size":"t-3r"}`, replicas: 3, size: "t-3r"},
	"upgrade":     {name: "upgrade server 26.8.34.13 -> 26.8.36.3", body: `{"version":"26.8.36.3"}`, version: "26.8.36.3"},
	"scale":       {name: "vertical scale (t-3r -> t-4r)", body: `{"size":"t-4r"}`, size: "t-4r"},
}

func (e *env) patch(t *testing.T, c cluster, body string) string {
	t.Helper()
	req, _ := http.NewRequest("PATCH", e.api+"/v1/projects/"+c.project+"/clusters/"+c.id, bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "life-"+time.Now().Format("150405.000")+"-"+body)
	resp, err := (&http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 202 {
		t.Fatalf("PATCH %s: %d %s", body, resp.StatusCode, b)
	}
	var op struct{ ID string }
	if json.Unmarshal(b, &op) != nil || op.ID == "" {
		t.Fatalf("no operation in %s", b)
	}
	return op.ID
}

func (e *env) waitOp(t *testing.T, c cluster, opID string, limit time.Duration) (state, errMsg string) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		_, b := e.apiGet("/v1/projects/"+c.project+"/operations/"+opID, c.token)
		var op struct{ State, Error string }
		if json.Unmarshal(b, &op) == nil && (op.State == "SUCCEEDED" || op.State == "FAILED") {
			return op.State, op.Error
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("operation %s still running after %v", opID, limit)
	return "", ""
}

type clusterView struct {
	State, StateReason string
	Spec               struct {
		Size, ServerVersion, KeeperVersion string
		ReplicasPerShard                   int
	}
}

func (e *env) describe(t *testing.T, c cluster) clusterView {
	t.Helper()
	st, b := e.apiGet("/v1/projects/"+c.project+"/clusters/"+c.id, c.token)
	var v clusterView
	if st != 200 || json.Unmarshal(b, &v) != nil {
		t.Fatalf("describe: %d %s", st, b)
	}
	return v
}

func TestLifecycleUnderLoad(t *testing.T) {
	e := setup(t)
	quorum := os.Getenv("LIFE_QUORUM")
	if quorum == "" {
		quorum = "auto"
	}
	names := strings.Split(envOr("LIFE_STEPS", "add-replica,upgrade,scale"), ",")

	v := newVerifier(e, quorum)
	v.Setup(t)
	t.Cleanup(func() { e.admin(e.a).q("DROP TABLE IF EXISTS " + verifyTable + " ON CLUSTER default SYNC") })
	v.Start()
	stopped := false
	defer func() {
		if !stopped {
			v.Stop()
		}
	}()

	time.Sleep(60 * time.Second)
	base := v.Between(time.Now().Add(-45*time.Second), time.Now())
	t.Logf("baseline (insert_quorum=%s): %s", quorum, base)
	if base.Acked < 20 || len(base.Codes) > 0 {
		t.Fatalf("the load is not healthy before any change: %s", base)
	}

	replicas := e.describe(t, e.a).Spec.ReplicasPerShard
	version := e.describe(t, e.a).Spec.ServerVersion
	for _, n := range names {
		st, ok := allSteps[strings.TrimSpace(n)]
		if !ok {
			t.Fatalf("unknown step %q", n)
		}
		t.Run(st.name, func(t *testing.T) {
			if cv := e.describe(t, e.a); cv.State != "ACTIVE" {
				t.Fatalf("cluster is %s before the change", cv.State)
			}
			t0 := time.Now()
			opID := e.patch(t, e.a, st.body)
			t.Logf("operation %s started", opID)
			state, msg := e.waitOp(t, e.a, opID, 50*time.Minute)
			t1 := time.Now()
			time.Sleep(30 * time.Second) // keep the load on while the cluster settles
			w := v.Between(t0, t1)
			t.Logf("%s took %v; during it: %s", st.name, t1.Sub(t0).Round(time.Second), w)
			for code, m := range w.FirstMsg {
				t.Logf("  example of error code %d: %s", code, m)
			}
			after := v.Between(t1, time.Now())
			t.Logf("30 s after it: %s", after)
			if state != "SUCCEEDED" {
				t.Fatalf("operation %s: %s", state, msg)
			}
			cv := e.describe(t, e.a)
			if cv.State != "ACTIVE" {
				t.Errorf("cluster is %s after the change", cv.State)
			}
			if st.replicas != 0 {
				replicas = st.replicas
			}
			if st.version != "" {
				version = st.version
			}
			if st.size != "" && cv.Spec.Size != st.size {
				t.Errorf("size %s, want %s", cv.Spec.Size, st.size)
			}
			if cv.Spec.ReplicasPerShard != replicas || cv.Spec.ServerVersion != version {
				t.Errorf("spec %+v, want %d replicas on %s", cv.Spec, replicas, version)
			}
			for r := 0; r < replicas; r++ {
				out, err := e.a.podSQL(r, "SELECT version()")
				if err != nil || out != version {
					t.Errorf("replica %d runs %q (%v), want %s", r, out, err, version)
				}
				if ps := e.a.status(r); !ps.Ready {
					t.Errorf("replica %d: %+v", r, ps)
				}
			}
		})
	}

	time.Sleep(20 * time.Second)
	v.Stop()
	stopped = true
	whole := v.Between(time.Time{}, time.Now())
	t.Logf("whole run: %s", whole)
	checks := v.Verify(t, replicas)
	for _, c := range checks {
		t.Logf("replica %d holds %d batches: missing %d, partial %d, duplicated %d, present but never acknowledged %d %v",
			c.Replica, c.Batches, c.Missing, c.Partial, c.Duplicated, c.Unacked, c.MissingIDs)
		if c.Missing > 0 || c.Partial > 0 || c.Duplicated > 0 {
			t.Errorf("replica %d lost or corrupted acknowledged data", c.Replica)
		}
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
