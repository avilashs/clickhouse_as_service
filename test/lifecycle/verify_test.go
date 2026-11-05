//go:build lifecycle

package lifecycle

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Restore verification (M5): a backup is "verified" only when it restores into a scratch cluster
// and the data matches. These tests check both directions: a good backup verifies, and a backup
// that was damaged in the store does not.

type verificationView struct {
	ID, State string
	Scheduled bool
	Detail    struct {
		Tables, Exact, Bounded int
		Problems               []string
	}
}

func (e *env) verifications(t *testing.T, c cluster, backup string) []verificationView {
	t.Helper()
	st, b := e.call(t, "GET", "/v1/projects/"+c.project+"/clusters/"+c.id+"/backups/"+backup+"/verifications", c.token, "")
	var l struct{ Verifications []verificationView }
	if st != 200 || json.Unmarshal(b, &l) != nil {
		t.Fatalf("verifications: %d %s", st, b)
	}
	return l.Verifications
}

// waitVerification waits for the newest verification of a backup to finish.
func (e *env) waitVerification(t *testing.T, c cluster, backup string, limit time.Duration) verificationView {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if vs := e.verifications(t, c, backup); len(vs) > 0 && vs[0].State != "RUNNING" {
			return vs[0]
		}
		time.Sleep(10 * time.Second)
	}
	t.Fatalf("no finished verification of %s after %v", backup, limit)
	return verificationView{}
}

func namespaces(t *testing.T) int {
	t.Helper()
	out, _ := kubectl("get", "ns", "-o", "name")
	n := 0
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "namespace/ch-") {
			n++
		}
	}
	return n
}

func (e *env) seedForVerification(t *testing.T, c cluster) {
	t.Helper()
	cl := e.admin(c)
	must(t, cl.q("DROP TABLE IF EXISTS default.bk_events ON CLUSTER default SYNC"))
	must(t, cl.q("CREATE TABLE default.bk_events ON CLUSTER default (batch UInt64, i UInt32, payload String) ENGINE = ReplicatedMergeTree ORDER BY (batch, i)"))
	e.insertBatches(t, c, 1, 10)
}

func TestVerifyGoodBackup(t *testing.T) {
	e := setup(t)
	a := e.a
	e.seedForVerification(t, a)
	bk := e.takeBackup(t, a, `{"kind":"FULL"}`)
	before := namespaces(t)
	e.startVerify(t, a, bk.ID)
	t0 := time.Now()
	v := e.waitVerification(t, a, bk.ID, 45*time.Minute)
	t.Logf("verification %s: %s after %v: %d tables, %d exact, %d bounded, problems %v", v.ID, v.State, time.Since(t0).Round(time.Second), v.Detail.Tables, v.Detail.Exact, v.Detail.Bounded, v.Detail.Problems)
	if v.State != "PASSED" || v.Detail.Tables == 0 || len(v.Detail.Problems) != 0 {
		t.Fatalf("a good backup did not verify: %+v", v)
	}
	if !e.backup(t, a, bk.ID).Verified {
		t.Error("the backup is not marked verified")
	}
	time.Sleep(20 * time.Second)
	if after := namespaces(t); after > before {
		t.Errorf("the scratch cluster was left behind: %d namespaces before, %d after", before, after)
	}
}

func (e *env) startVerify(t *testing.T, c cluster, backup string) {
	t.Helper()
	st, b := e.call(t, "POST", "/v1/projects/"+c.project+"/clusters/"+c.id+"/backups/"+backup+"/verify", c.token, "")
	if st != 202 {
		t.Fatalf("verify: %d %s", st, b)
	}
}

func TestVerifyDamagedBackup(t *testing.T) {
	e := setup(t)
	a := e.a
	ak, sk, endpoint, bucket := os.Getenv("LIFE_STORE_ACCESS_KEY"), os.Getenv("LIFE_STORE_SECRET_KEY"), os.Getenv("LIFE_STORE_ENDPOINT"), os.Getenv("LIFE_STORE_BUCKET")
	if ak == "" || endpoint == "" {
		t.Skip("LIFE_STORE_* not set")
	}
	e.seedForVerification(t, a)
	bk := e.takeBackup(t, a, `{"kind":"FULL"}`)

	s3, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(ak, sk, "")})
	if err != nil {
		t.Fatal(err)
	}
	prefix := a.project + "/" + a.id + "/" + bk.ID + "/"
	var victim string
	var size int64
	for o := range s3.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if o.Err != nil {
			t.Fatal(o.Err)
		}
		if !strings.HasSuffix(o.Key, ".backup") && o.Size > size {
			victim, size = o.Key, o.Size
		}
	}
	if victim == "" {
		t.Fatal("no data object to damage")
	}
	t.Logf("damaging %s (%d bytes): replacing it with garbage of the same size", victim, size)
	junk := strings.NewReader(strings.Repeat("x", int(size)))
	if _, err := s3.PutObject(context.Background(), bucket, victim, junk, size, minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}

	before := namespaces(t)
	e.startVerify(t, a, bk.ID)
	v := e.waitVerification(t, a, bk.ID, 45*time.Minute)
	t.Logf("verification %s: %s, problems %v", v.ID, v.State, v.Detail.Problems)
	if v.State != "FAILED" {
		t.Fatalf("a damaged backup verified: %+v", v)
	}
	if e.backup(t, a, bk.ID).Verified {
		t.Error("a damaged backup is marked verified")
	}
	time.Sleep(20 * time.Second)
	if after := namespaces(t); after > before {
		t.Errorf("the scratch cluster was left behind: %d namespaces before, %d after", before, after)
	}
}

// TestScheduledBackupAndVerification: nobody asks for anything. With the worker started with
// BASALT_BACKUP_INTERVAL and BASALT_VERIFY_INTERVAL set and the cluster holding data, the scheduler
// takes a backup, and then restores it into a scratch cluster and verifies it. Run it first on a
// fresh cluster, before any manual backups exist.
func TestScheduledBackupAndVerification(t *testing.T) {
	e := setup(t)
	a := e.a
	e.seedForVerification(t, a)
	t0 := time.Now()
	var bk backupView
	deadline := time.Now().Add(15 * time.Minute)
	for time.Now().Before(deadline) {
		st, b := e.call(t, "GET", "/v1/projects/"+a.project+"/clusters/"+a.id+"/backups", a.token, "")
		var l struct{ Backups []backupView }
		if st == 200 && json.Unmarshal(b, &l) == nil {
			for _, x := range l.Backups {
				if x.Scheduled && x.State == "COMPLETED" {
					bk = x
				}
			}
		}
		if bk.ID != "" {
			break
		}
		time.Sleep(10 * time.Second)
	}
	if bk.ID == "" {
		t.Fatal("the scheduler took no backup")
	}
	t.Logf("scheduled backup %s (%s) completed %v after the test started", bk.ID, bk.Kind, time.Since(t0).Round(time.Second))
	v := e.waitVerification(t, a, bk.ID, 60*time.Minute)
	t.Logf("scheduled verification %s: %s after %v: %d tables, %d exact, %d bounded, scheduled=%v, problems %v",
		v.ID, v.State, time.Since(t0).Round(time.Second), v.Detail.Tables, v.Detail.Exact, v.Detail.Bounded, v.Scheduled, v.Detail.Problems)
	if !v.Scheduled || v.State != "PASSED" {
		t.Fatalf("scheduled verification: %+v", v)
	}
	if !e.backup(t, a, bk.ID).Verified {
		t.Error("the backup is not marked verified")
	}
}
