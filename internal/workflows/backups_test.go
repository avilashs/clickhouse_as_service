package workflows

import (
	"strings"
	"testing"
	"time"

	"github.com/avilashs/clickhouse_as_service/internal/model"
)

func fp(rows uint64, hash string) model.TableFingerprint {
	return model.TableFingerprint{Rows: rows, Hash: hash}
}

func TestCompareFingerprintsExactWhenNobodyWrote(t *testing.T) {
	src := model.JobResult{Before: map[string]model.TableFingerprint{"d.t": fp(100, "h1")}, After: map[string]model.TableFingerprint{"d.t": fp(100, "h1")}}
	got := model.JobResult{Restored: map[string]model.TableFingerprint{"d.t": fp(100, "h1")}, Checks: map[string]string{"d.t": ""}}
	if d := CompareFingerprints(src, got); len(d.Problems) != 0 || d.Exact != 1 || d.Bounded != 0 {
		t.Fatalf("%+v", d)
	}
}

func TestCompareFingerprintsCatchesWhatIsWrong(t *testing.T) {
	src := model.JobResult{
		Before: map[string]model.TableFingerprint{"d.same": fp(10, "a"), "d.moved": fp(10, "a"), "d.gone": fp(1, "x"), "d.bad": fp(5, "q")},
		After:  map[string]model.TableFingerprint{"d.same": fp(10, "a"), "d.moved": fp(20, "b"), "d.gone": fp(1, "x"), "d.bad": fp(5, "q")},
	}
	got := model.JobResult{
		Restored: map[string]model.TableFingerprint{
			"d.same":  fp(10, "DIFFERENT"), // same rows, different content: corruption
			"d.moved": fp(25, "z"),         // more than the source ever held
			"d.bad":   fp(5, "q"),
			"d.extra": fp(1, "e"),
		},
		Checks: map[string]string{"d.bad": "part all_1_1_0: checksum mismatch"},
	}
	d := CompareFingerprints(src, got)
	want := []string{"d.same: restored 10 rows", "d.moved: restored 25 rows, outside", "d.gone: missing", "d.bad: CHECK TABLE", "d.extra: restored but not"}
	joined := strings.Join(d.Problems, "\n")
	for _, w := range want {
		if !strings.Contains(joined, w) {
			t.Errorf("no problem reported for %q in:\n%s", w, joined)
		}
	}
	if len(d.Problems) != len(want) {
		t.Errorf("%d problems, want %d:\n%s", len(d.Problems), len(want), joined)
	}
}

func TestCompareFingerprintsBoundsATableWrittenDuringTheBackup(t *testing.T) {
	src := model.JobResult{Before: map[string]model.TableFingerprint{"d.t": fp(100, "a")}, After: map[string]model.TableFingerprint{"d.t": fp(160, "b")}}
	ok := model.JobResult{Restored: map[string]model.TableFingerprint{"d.t": fp(130, "mid")}}
	if d := CompareFingerprints(src, ok); len(d.Problems) != 0 || d.Bounded != 1 {
		t.Fatalf("%+v", d)
	}
	for _, rows := range []uint64{99, 161} {
		bad := model.JobResult{Restored: map[string]model.TableFingerprint{"d.t": fp(rows, "x")}}
		if d := CompareFingerprints(src, bad); len(d.Problems) != 1 {
			t.Errorf("%d rows should be outside the bounds: %+v", rows, d)
		}
	}
}

func TestCompareFingerprintsRefusesAnEmptyRecord(t *testing.T) {
	if d := CompareFingerprints(model.JobResult{}, model.JobResult{}); len(d.Problems) == 0 {
		t.Fatal("a backup that recorded no tables must not verify")
	}
}

func TestNextBackupKind(t *testing.T) {
	now := time.Now()
	full := func(id string, age int) model.Backup {
		return model.Backup{ID: id, State: model.BackupCompleted, Kind: model.BackupFull, CreatedAt: now.Add(-time.Duration(age) * time.Hour)}
	}
	inc := func(id, base string, age int) model.Backup {
		return model.Backup{ID: id, State: model.BackupCompleted, Kind: model.BackupIncremental, BaseBackupID: base, CreatedAt: now.Add(-time.Duration(age) * time.Hour)}
	}
	if k, _ := NextBackupKind(nil, 7); k != model.BackupFull {
		t.Fatalf("first backup is %s", k)
	}
	// newest first, as the store returns them
	chain := []model.Backup{inc("c", "b", 1), inc("b", "a", 2), full("a", 3)}
	if k, base := NextBackupKind(chain, 7); k != model.BackupIncremental || base != "c" {
		t.Fatalf("got %s on %s", k, base)
	}
	if k, _ := NextBackupKind(chain, 3); k != model.BackupFull {
		t.Fatalf("a chain of 3 with FullEvery 3 must start a new full backup, got %s", k)
	}
	if k, _ := NextBackupKind(chain, 1); k != model.BackupFull {
		t.Fatalf("FullEvery 1 means every backup is full, got %s", k)
	}
	failedOnTop := append([]model.Backup{{ID: "f", State: model.BackupFailed, CreatedAt: now}}, chain...)
	if k, base := NextBackupKind(failedOnTop, 7); k != model.BackupIncremental || base != "c" {
		t.Fatalf("a failed backup is never a base: got %s on %s", k, base)
	}
}

func TestBackupPathIsScopedToTheCluster(t *testing.T) {
	p := BackupPath("demo", "c123", "b456")
	if p != "demo/c123/b456" || !strings.HasPrefix(p, "demo/c123/") {
		t.Fatal(p)
	}
	if got := jobURL(model.BackupTarget{URL: "http://s3:3900/bucket/"}, p); got != "http://s3:3900/bucket/demo/c123/b456/" {
		t.Fatal(got)
	}
}
