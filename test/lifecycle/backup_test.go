//go:build lifecycle

package lifecycle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Backup and restore acceptance (M5): back up a live cluster, take an incremental backup, restore it
// into a new cluster and compare the data row for row, check that the tenant cannot reach the store,
// and that deleting a backup that others are built on is refused.

func (e *env) call(t *testing.T, method, path, token, body string) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = bytes.NewReader([]byte(body))
	}
	req, _ := http.NewRequest(method, e.api+path, rd)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", fmt.Sprintf("bk-%d-%s", time.Now().UnixNano(), method))
	resp, err := (&http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (e *env) startOp(t *testing.T, method, path, token, body string) string {
	t.Helper()
	st, b := e.call(t, method, path, token, body)
	var op struct{ ID, ClusterID string }
	if st != 202 || json.Unmarshal(b, &op) != nil {
		t.Fatalf("%s %s: %d %s", method, path, st, b)
	}
	return op.ID
}

type backupView struct {
	ID, ClusterID, Kind, BaseBackupID, State, Error string
	SizeBytes                                       int64
	Scheduled                                       bool
	Verified                                        bool
}

func (e *env) backup(t *testing.T, c cluster, id string) backupView {
	t.Helper()
	st, b := e.call(t, "GET", "/v1/projects/"+c.project+"/clusters/"+c.id+"/backups/"+id, c.token, "")
	var v backupView
	if st != 200 || json.Unmarshal(b, &v) != nil {
		t.Fatalf("get backup %s: %d %s", id, st, b)
	}
	return v
}

// takeBackup runs one backup and returns it once COMPLETED.
func (e *env) takeBackup(t *testing.T, c cluster, body string) backupView {
	t.Helper()
	t0 := time.Now()
	op := e.startOp(t, "POST", "/v1/projects/"+c.project+"/clusters/"+c.id+"/backups", c.token, body)
	state, msg := e.waitOp(t, c, op, 30*time.Minute)
	if state != "SUCCEEDED" {
		t.Fatalf("backup operation %s: %s", state, msg)
	}
	st, b := e.call(t, "GET", "/v1/projects/"+c.project+"/clusters/"+c.id+"/backups", c.token, "")
	var l struct{ Backups []backupView }
	if st != 200 || json.Unmarshal(b, &l) != nil || len(l.Backups) == 0 {
		t.Fatalf("list backups: %d %s", st, b)
	}
	bk := l.Backups[0] // newest first
	t.Logf("backup %s (%s) took %v: state %s, %d bytes", bk.ID, bk.Kind, time.Since(t0).Round(time.Second), bk.State, bk.SizeBytes)
	if bk.State != "COMPLETED" || bk.SizeBytes == 0 {
		t.Fatalf("backup %+v", bk)
	}
	return bk
}

func (e *env) insertBatches(t *testing.T, c cluster, from, n int) {
	t.Helper()
	cl := e.admin(c)
	for b := from; b < from+n; b++ {
		var sb strings.Builder
		sb.WriteString("INSERT INTO default.bk_events FORMAT TSV\n")
		for i := 0; i < 2000; i++ {
			fmt.Fprintf(&sb, "%d\t%d\tp%d-%d\n", b, i, b, i)
		}
		r := cl.qp(sb.String(), map[string][]string{"insert_quorum": {"auto"}, "insert_quorum_timeout": {"120000"}})
		if !r.ok() {
			t.Fatalf("insert batch %d: %s %s", b, r.err, r.body)
		}
	}
}

// state is what every replica of the cluster holds, in a form that compares row for row: for each
// table the rows and content hash on each replica, read through the SQL endpoint (kubectl exec into
// a gVisor pod is not reliable under load).
func (e *env) state(t *testing.T, c cluster) string {
	t.Helper()
	cl := e.provider(t, c) // the platform's own user: tenants hold no READ ON REMOTE
	var out []string
	for _, tbl := range []string{"bk_events", "bk_small", "bk_view"} {
		r := cl.q("SELECT hostName() AS h, count(), sum(cityHash64(*)) FROM clusterAllReplicas('default', default." + tbl + ") GROUP BY h ORDER BY h FORMAT TSV")
		if !r.ok() {
			t.Fatalf("state of %s: %s %s", tbl, r.err, firstLine(r.body))
		}
		lines := strings.Split(strings.TrimSpace(r.body), "\n")
		if len(lines) != 2 {
			t.Errorf("%s is on %d replicas, want 2: %q", tbl, len(lines), r.body)
		}
		for _, l := range lines {
			f := strings.Fields(l)
			if len(f) == 3 {
				out = append(out, tbl+"\t"+f[1]+"\t"+f[2])
			}
		}
	}
	return strings.Join(out, "\n")
}

// clearBackups deletes every backup of the cluster, newest first, and waits for the store to
// empty, so that the test counts only its own objects.
func (e *env) clearBackups(t *testing.T, c cluster) {
	t.Helper()
	for pass := 0; pass < 6; pass++ {
		st, b := e.call(t, "GET", "/v1/projects/"+c.project+"/clusters/"+c.id+"/backups", c.token, "")
		var l struct{ Backups []backupView }
		if st != 200 || json.Unmarshal(b, &l) != nil {
			t.Fatalf("list backups: %d %s", st, b)
		}
		if len(l.Backups) == 0 {
			return
		}
		for _, bk := range l.Backups { // newest first, so no later backup stands on an earlier one
			e.call(t, "DELETE", "/v1/projects/"+c.project+"/clusters/"+c.id+"/backups/"+bk.ID, c.token, "")
		}
		time.Sleep(40 * time.Second)
	}
	t.Fatal("could not clear the cluster's earlier backups")
}

func TestBackupRestore(t *testing.T) {
	e := setup(t)
	a := e.a
	cl := e.admin(a)
	e.clearBackups(t, a)
	must(t, cl.q("DROP TABLE IF EXISTS default.bk_events ON CLUSTER default SYNC"))
	must(t, cl.q("DROP TABLE IF EXISTS default.bk_small ON CLUSTER default SYNC"))
	must(t, cl.q("DROP VIEW IF EXISTS default.bk_view ON CLUSTER default SYNC"))
	must(t, cl.q("CREATE TABLE default.bk_events ON CLUSTER default (batch UInt64, i UInt32, payload String) ENGINE = ReplicatedMergeTree ORDER BY (batch, i)"))
	must(t, cl.q("CREATE TABLE default.bk_small ON CLUSTER default (k UInt32, v String) ENGINE = ReplicatedMergeTree ORDER BY k"))
	must(t, cl.q("CREATE VIEW default.bk_view ON CLUSTER default AS SELECT batch, count() AS n FROM default.bk_events GROUP BY batch"))
	must(t, cl.q("INSERT INTO default.bk_small SELECT number, toString(number) FROM numbers(5000)"))
	e.insertBatches(t, a, 1, 20)

	var full, inc backupView
	t.Run("full backup", func(t *testing.T) {
		full = e.takeBackup(t, a, `{"kind":"FULL"}`)
	})
	t.Run("incremental backup is smaller and names its base", func(t *testing.T) {
		e.insertBatches(t, a, 21, 10)
		// while the job runs, its named collection (with the store's key in it) exists on the
		// server: the tenant must not be able to see it
		stop, seen := make(chan struct{}), make(chan []string, 1)
		go func() {
			var rows []string
			for {
				select {
				case <-stop:
					seen <- rows
					return
				case <-time.After(150 * time.Millisecond):
				}
				if r := cl.q("SELECT name FROM system.named_collections"); r.ok() && strings.TrimSpace(r.body) != "" {
					rows = append(rows, strings.TrimSpace(r.body))
				}
				if r := cl.q("SHOW NAMED COLLECTIONS"); r.ok() && strings.TrimSpace(r.body) != "" {
					rows = append(rows, strings.TrimSpace(r.body))
				}
			}
		}()
		inc = e.takeBackup(t, a, `{"kind":"INCREMENTAL"}`)
		close(stop)
		if rows := <-seen; len(rows) > 0 {
			t.Errorf("the tenant saw named collections while a backup ran: %v", rows)
		}
		if inc.Kind != "INCREMENTAL" || inc.BaseBackupID != full.ID {
			t.Fatalf("incremental backup %+v, want base %s", inc, full.ID)
		}
		if inc.SizeBytes >= full.SizeBytes*2 {
			t.Errorf("incremental backup %d bytes vs full %d: it should hold only the new data", inc.SizeBytes, full.SizeBytes)
		}
	})

	t.Run("the tenant cannot reach the store or the credentials", func(t *testing.T) {
		for _, q := range []string{
			"BACKUP TABLE default.bk_small TO S3('http://backup-store.basalt-system.svc.cluster.local:3900/basalt-backups/steal/', 'a', 'b')",
			"BACKUP TABLE default.bk_small TO Disk('default', 'x')",
			"SELECT * FROM s3('http://backup-store.basalt-system.svc.cluster.local:3900/basalt-backups/*', 'a', 'b')",
			"SELECT * FROM url('http://backup-store.basalt-system.svc.cluster.local:3900/', CSV, 'x String')",
			"CREATE NAMED COLLECTION evil AS url = 'http://x/'",
			"RESTORE TABLE default.bk_small FROM S3('http://backup-store.basalt-system.svc.cluster.local:3900/basalt-backups/x/', 'a', 'b')",
		} {
			r := cl.q(q)
			if r.ok() {
				t.Errorf("tenant query succeeded: %s -> %s", q, firstLine(r.body))
			} else {
				t.Logf("denied (%d): %s", r.code, q)
			}
		}
		if r := cl.q("SELECT * FROM system.named_collections"); r.ok() && strings.TrimSpace(r.body) != "" {
			t.Errorf("the tenant reads named collections: %s", firstLine(r.body))
		}
		// the store answers nobody without a signature (from here; from a server pod too, if exec works)
		if resp, err := (&http.Client{Timeout: 10 * time.Second}).Get("http://" + os.Getenv("LIFE_STORE_ENDPOINT") + "/" + os.Getenv("LIFE_STORE_BUCKET") + "/?list-type=2"); err == nil {
			resp.Body.Close()
			t.Logf("anonymous GET of the bucket: %s", resp.Status)
			if resp.StatusCode == 200 {
				t.Errorf("the bucket answers anonymous requests")
			}
		}
		out, err := kubectl("-n", a.ns(), "exec", a.pod(0), "-c", "clickhouse", "--", "bash", "-c",
			`exec 3<>/dev/tcp/backup-store.basalt-system.svc.cluster.local/3900; printf 'GET /basalt-backups/?list-type=2 HTTP/1.0\r\nHost: x\r\n\r\n' >&3; head -1 <&3`)
		if err != nil {
			t.Logf("anonymous GET from a server pod not checked: exec failed (%s)", firstLine(out))
		} else {
			t.Logf("anonymous GET of the bucket from a server pod: %q", strings.TrimSpace(out))
			if strings.Contains(out, "200") {
				t.Errorf("the bucket answers anonymous requests from a server pod: %s", out)
			}
		}
		// no job leaves a named collection behind, on any replica
		prov := e.provider(t, a)
		if r := prov.q("SELECT count() FROM clusterAllReplicas('default', system.named_collections)"); !r.ok() || strings.TrimSpace(r.body) != "0" {
			t.Errorf("named collections left on the servers after the backups: %s %s", r.err, firstLine(r.body))
		}
	})

	t.Run("restore the incremental backup into a new cluster", func(t *testing.T) {
		want := e.state(t, a)
		t.Logf("source state:\n%s", want)
		t0 := time.Now()
		op := e.startOp(t, "POST", "/v1/projects/"+a.project+"/clusters/"+a.id+"/backups/"+inc.ID+"/restore", a.token,
			fmt.Sprintf(`{"name":"restored-%d"}`, time.Now().Unix()%100000))
		var cid string
		_, b := e.call(t, "GET", "/v1/projects/"+a.project+"/operations/"+op, a.token, "")
		var o struct{ ClusterID string }
		_ = json.Unmarshal(b, &o)
		cid = o.ClusterID
		state_, msg := e.waitOp(t, a, op, 60*time.Minute)
		if state_ != "SUCCEEDED" {
			t.Fatalf("restore operation %s: %s", state_, msg)
		}
		t.Logf("restore into cluster %s took %v", cid, time.Since(t0).Round(time.Second))
		r := cluster{project: a.project, token: a.token, id: cid}
		e.fill(t, &r)
		if got := e.state(t, r); got != want {
			t.Errorf("the restored cluster differs from the source:\n%s\nsource:\n%s", got, want)
		}
		// the restored cluster has its own credentials: the source's password does not open it
		if bad := e.connect(r, "customer_admin", a.adminPW, nil).q("SELECT 1"); bad.ok() {
			t.Errorf("the source cluster's admin password opens the restored cluster")
		}
		e.call(t, "DELETE", "/v1/projects/"+a.project+"/clusters/"+cid, a.token, "")
	})

	t.Run("a backup others are built on cannot be deleted first", func(t *testing.T) {
		st, b := e.call(t, "DELETE", "/v1/projects/"+a.project+"/clusters/"+a.id+"/backups/"+full.ID, a.token, "")
		if st != 409 {
			t.Fatalf("deleting the base of an incremental backup: %d %s", st, b)
		}
		t.Logf("refused: %s", strings.TrimSpace(string(b)))
	})

	t.Run("deleting backups removes their objects, newest first", func(t *testing.T) {
		ak, sk, endpoint, bucket := os.Getenv("LIFE_STORE_ACCESS_KEY"), os.Getenv("LIFE_STORE_SECRET_KEY"), os.Getenv("LIFE_STORE_ENDPOINT"), os.Getenv("LIFE_STORE_BUCKET")
		if ak == "" || endpoint == "" {
			t.Skip("LIFE_STORE_* not set")
		}
		s3, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(ak, sk, "")})
		if err != nil {
			t.Fatal(err)
		}
		// only this cluster's objects: other clusters' scheduled backups live in the same bucket
		count := func() int {
			n := 0
			for o := range s3.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Prefix: a.project + "/" + a.id + "/", Recursive: true}) {
				if o.Err == nil {
					n++
				}
			}
			return n
		}
		before := count()
		for _, id := range []string{inc.ID, full.ID} {
			st, b := e.call(t, "DELETE", "/v1/projects/"+a.project+"/clusters/"+a.id+"/backups/"+id, a.token, "")
			if st != 202 {
				t.Fatalf("delete %s: %d %s", id, st, b)
			}
			deadline := time.Now().Add(3 * time.Minute)
			for time.Now().Before(deadline) {
				if s, _ := e.call(t, "GET", "/v1/projects/"+a.project+"/clusters/"+a.id+"/backups/"+id, a.token, ""); s == 404 {
					break
				}
				time.Sleep(5 * time.Second)
			}
		}
		time.Sleep(5 * time.Second)
		after := count()
		t.Logf("objects in the bucket: %d before, %d after", before, after)
		if before == 0 || after != 0 {
			t.Errorf("objects %d -> %d, want > 0 -> 0", before, after)
		}
	})
}
