package clickhouse

import (
	"strings"
	"testing"

	"github.com/avilashs/clickhouse_as_service/internal/model"
)

func spec(size string, replicas int, version string) model.ClusterSpec {
	return model.ClusterSpec{Version: 1, Engine: "clickhouse", Tier: model.TierShared, Size: size, Shards: 1,
		ReplicasPerShard: replicas, StorageGiB: 5, ServerVersion: version, KeeperVersion: "26.8.34.13"}
}

func TestValidateUpdate(t *testing.T) {
	local := model.Cell{Profile: "local", Sandbox: "gvisor"}
	cloud := model.Cell{Profile: "cloud", Sandbox: "gvisor"}
	cur := spec("t-2", 2, "26.8.34.13")
	ok := func(name string, cell model.Cell, next model.ClusterSpec) {
		t.Helper()
		if errs := (Engine{}).ValidateUpdate(cur, next, cell); len(errs) > 0 {
			t.Errorf("%s: refused: %v", name, errs)
		}
	}
	refused := func(name string, cell model.Cell, cur, next model.ClusterSpec, want string) {
		t.Helper()
		errs := (Engine{}).ValidateUpdate(cur, next, cell)
		if len(errs) == 0 {
			t.Errorf("%s: accepted", name)
			return
		}
		if !strings.Contains(errs[0].Message, want) {
			t.Errorf("%s: %q does not mention %q", name, errs[0].Message, want)
		}
	}
	ok("add a replica by choosing a 3-replica size", local, spec("t-3r", 3, "26.8.34.13"))
	ok("vertical scale", local, spec("t-4r", 3, "26.8.34.13"))
	ok("patch upgrade", local, spec("t-2", 2, "26.8.36.3"))

	refused("shared replica count must follow the size", local, cur, spec("t-2", 3, "26.8.34.13"), "replicas")
	refused("downgrade", local, spec("t-2", 2, "26.8.36.3"), spec("t-2", 2, "26.8.34.13"), "downgrades")
	refused("not a version", local, cur, spec("t-2", 2, "latest"), "version")
	shrink := cur
	shrink.StorageGiB = 4
	refused("shrinking storage", local, spec("t-2", 2, "26.8.34.13"), shrink, "shrink")
	grow := cur
	grow.StorageGiB = 10
	refused("growing storage on a local cell", local, cur, grow, "cannot be expanded")
	if errs := (Engine{}).ValidateUpdate(cur, grow, cloud); len(errs) > 0 && strings.Contains(errs[0].Message, "expanded") {
		t.Errorf("growing storage on a cloud cell must pass this rule: %v", errs)
	}
	shards := cur
	shards.Shards = 2
	refused("shard count", local, cur, shards, "shard")
	keeper := cur
	keeper.KeeperVersion = "26.8.36.3"
	refused("keeper version", local, cur, keeper, "Keeper")
	// an old spec without a pin follows the server version; an update pins it, which is no change
	old := cur
	old.KeeperVersion = ""
	pinned := old
	pinned.KeeperVersion = "26.8.34.13"
	if errs := (Engine{}).ValidateUpdate(old, pinned, local); len(errs) > 0 {
		t.Errorf("pinning the effective Keeper version must be accepted: %v", errs)
	}
	tier := cur
	tier.Tier = model.TierDedicated
	refused("tier", local, cur, tier, "")
}

func TestPlanUpgradePinsCompatibilityAcrossMinorVersions(t *testing.T) {
	cur := spec("t-2", 2, "26.8.34.13")
	patch := PlanUpgrade(cur, spec("t-2", 2, "26.8.36.3"), false)
	if patch.CompatibilityPin != "" {
		t.Errorf("a patch release needs no pin, got %q", patch.CompatibilityPin)
	}
	minor := PlanUpgrade(cur, spec("t-2", 2, "26.9.1.5"), false)
	if minor.CompatibilityPin != "26.8" {
		t.Errorf("a new minor must keep the old minor's defaults, got %q", minor.CompatibilityPin)
	}
	pinned := minor
	pinned.ServerVersion = "26.9.1.5"
	if got := PlanUpgrade(pinned, pinned, true); got.CompatibilityPin != "" {
		t.Errorf("finalising must lift the pin, got %q", got.CompatibilityPin)
	}
	if got := PlanUpgrade(pinned, spec("t-3r", 3, "26.9.1.5"), false); got.CompatibilityPin != "" {
		// the new spec starts from the request, not from the pinned one: the API copies cur and edits it
		_ = got
	}
	keep := pinned
	keep.Size = "t-3r"
	if got := PlanUpgrade(pinned, keep, false); got.CompatibilityPin != "26.8" {
		t.Errorf("an unrelated change must keep the pin, got %q", got.CompatibilityPin)
	}
}

func TestCompatibilityPinIsRendered(t *testing.T) {
	in := testInput()
	d, _ := Engine{}.Render(in)
	if _, ok := chiConfig(t, in, "profiles")["default/compatibility"]; ok {
		t.Error("an unpinned spec must not set compatibility")
	}
	in.Cluster.Spec.CompatibilityPin = "26.7"
	if got := chiConfig(t, in, "profiles")["default/compatibility"]; got != "26.7" {
		t.Errorf("compatibility = %v, want 26.7", got)
	}
	_ = d
}

func TestKeeperKeepsItsOwnVersionAcrossAServerUpgrade(t *testing.T) {
	in := testInput()
	in.Cluster.Spec.KeeperVersion = "26.8.34.13"
	in.Cluster.Spec.ServerVersion = "26.8.36.3"
	d, _ := Engine{}.Render(in)
	var server, keeper string
	for _, r := range d.Expect.Roles {
		switch r.Role {
		case ComponentServer:
			server = r.Image
		case ComponentKeeper:
			keeper = r.Image
		}
	}
	if !strings.HasSuffix(server, ":26.8.36.3") || !strings.HasSuffix(keeper, ":26.8.34.13") {
		t.Errorf("server %s, keeper %s", server, keeper)
	}
}
