package clickhouse

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/avilashs/clickhouse_as_service/internal/model"
)

func renderJSON(t *testing.T, in interface{}) string {
	t.Helper()
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Without a backup target nothing may be reachable from SQL and the pods get no egress beyond
// the namespace (the M3 invariant); with one, exactly that host and port, and nothing else.
func TestBackupTargetOpensExactlyOneDoor(t *testing.T) {
	in := testInput()
	none, err := Engine{}.Render(in)
	if err != nil {
		t.Fatal(err)
	}
	if s := renderJSON(t, none.Objects); !strings.Contains(s, denyAllHost) || strings.Contains(s, "ipBlock") {
		t.Fatal("a cell without a backup target must keep the deny-all host and open no egress")
	}

	in.Cell.Backup = &model.BackupTarget{URL: "http://store.example:3900/b/", Bucket: "b", Host: "store.example", Port: 3900,
		EgressNamespace: "basalt-system", EgressPodSelector: map[string]string{"app": "backup-store"}}
	with, err := Engine{}.Render(in)
	if err != nil {
		t.Fatal(err)
	}
	s := renderJSON(t, with.Objects)
	if strings.Contains(s, denyAllHost) || !strings.Contains(s, `"store.example"`) {
		t.Fatal("the allowed remote host must be the backup target and only that")
	}
	for _, want := range []string{`"app":"backup-store"`, `"port":3900`, `"kubernetes.io/metadata.name":"basalt-system"`} {
		if !strings.Contains(s, want) {
			t.Errorf("network policy lacks %s", want)
		}
	}
	// the keeper policy never gets the door
	for _, o := range with.Objects {
		if o["kind"] == "NetworkPolicy" && o["metadata"].(map[string]any)["name"] == "keeper" && strings.Contains(renderJSON(t, o), "3900") {
			t.Error("Keeper pods must not reach the backup store")
		}
	}
}

func TestBackupEgressByCIDR(t *testing.T) {
	cell := model.Cell{Backup: &model.BackupTarget{Port: 443, EgressCIDRs: []string{"10.1.2.0/24"}}}
	got := renderJSON(t, backupEgress(cell))
	if !strings.Contains(got, `"cidr":"10.1.2.0/24"`) || !strings.Contains(got, `"port":443`) {
		t.Fatal(got)
	}
	if backupEgress(model.Cell{}) != nil || backupEgress(model.Cell{Backup: &model.BackupTarget{Port: 1}}) != nil {
		t.Fatal("no target, or a target with nowhere to go, must open nothing")
	}
}
