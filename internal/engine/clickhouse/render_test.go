package clickhouse

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

var update = flag.Bool("update", false, "rewrite golden files")

func testInput() engine.RenderInput {
	return engine.RenderInput{
		Cluster: model.Cluster{
			ID: "cabc123def45", ProjectID: "p1", CellID: "cell1", Name: "orders", State: model.ClusterCreating,
			Spec: model.ClusterSpec{Version: 1, Engine: "clickhouse", Tier: model.TierShared, Size: "t-2",
				Shards: 1, ReplicasPerShard: 2, StorageGiB: 5, ServerVersion: "26.8.34.13"},
			CreatedAt: time.Unix(0, 0),
		},
		Cell: model.Cell{ID: "cell1", Profile: "local", Domain: "db.example.com"},
	}
}

func TestRenderGolden(t *testing.T) {
	d, err := Engine{}.Render(testInput())
	if err != nil {
		t.Fatal(err)
	}
	got, err := yaml.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", "t-2.golden.yaml")
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run go test ./internal/engine/clickhouse -update)", err)
	}
	if string(got) != string(want) {
		t.Fatalf("render differs from %s; review the diff and run with -update if intended", path)
	}
}

func TestRenderIsDeterministic(t *testing.T) {
	a, _ := Engine{}.Render(testInput())
	b, _ := Engine{}.Render(testInput())
	if a.Hash() != b.Hash() {
		t.Fatal("same input produced different payloads")
	}
	in := testInput()
	in.Cluster.Spec.StorageGiB = 6
	c, _ := Engine{}.Render(in)
	if c.Hash() == a.Hash() {
		t.Fatal("different spec produced the same hash")
	}
}

func find(t *testing.T, d engine.Desired, kind string) engine.Object {
	t.Helper()
	for _, o := range d.Objects {
		if o["kind"] == kind {
			return o
		}
	}
	t.Fatalf("no %s rendered", kind)
	return nil
}

func dig(o any, path ...string) any {
	for _, p := range path {
		m, ok := o.(map[string]any)
		if !ok {
			return nil
		}
		o = m[p]
	}
	return o
}

func TestServerPodInvariants(t *testing.T) {
	d, _ := Engine{}.Render(testInput())
	chi := find(t, d, "ClickHouseInstallation")
	pt := dig(chi, "spec", "templates", "podTemplates").([]any)[0]
	spec := dig(pt, "spec").(map[string]any)

	if spec["automountServiceAccountToken"] != false {
		t.Error("server pods must not mount a ServiceAccount token")
	}
	if dig(spec, "nodeSelector", "basalt.io/pool") != "shared" {
		t.Error("shared-tier pods must select the shared pool")
	}
	req := dig(spec, "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution").([]any)
	term := req[0].(map[string]any)
	if term["topologyKey"] != "kubernetes.io/hostname" || dig(term, "labelSelector", "matchLabels", "clickhouse.altinity.com/shard") != "0" {
		t.Error("replicas of a shard must have required hostname anti-affinity")
	}
	c := spec["containers"].([]any)[0].(map[string]any)
	res := c["resources"].(map[string]any)
	if dig(res, "requests", "cpu") != dig(res, "limits", "cpu") || dig(res, "requests", "memory") != dig(res, "limits", "memory") {
		t.Error("server pods must have requests == limits (Guaranteed QoS)")
	}
	if dig(c, "securityContext", "allowPrivilegeEscalation") != false {
		t.Error("privilege escalation must be off")
	}
	if dig(spec, "securityContext", "runAsNonRoot") != true {
		t.Error("must run as non-root")
	}
}

func TestNoPlaintextPorts(t *testing.T) {
	d, _ := Engine{}.Render(testInput())
	settings := dig(find(t, d, "ClickHouseInstallation"), "spec", "configuration", "settings").(map[string]any)
	for _, p := range []string{"http_port", "tcp_port", "interserver_http_port", "mysql_port", "postgresql_port"} {
		if settings[p] != "_removed_" {
			t.Errorf("%s must be removed, got %v", p, settings[p])
		}
	}
	cl := dig(find(t, d, "ClickHouseKeeperInstallation"), "spec", "configuration", "clusters").([]any)[0].(map[string]any)
	if cl["insecure"] != "no" || cl["secure"] != "yes" {
		t.Error("Keeper must be TLS-only")
	}
}

func TestConstraintsInDefaultProfile(t *testing.T) {
	d, _ := Engine{}.Render(testInput())
	p := dig(find(t, d, "ClickHouseInstallation"), "spec", "configuration", "profiles").(map[string]any)
	for _, k := range []string{
		"default/constraints/max_memory_usage/max",
		"default/constraints/max_threads/max",
		"default/constraints/database_replicated_allow_only_replicated_engine/const",
	} {
		if _, ok := p[k]; !ok {
			t.Errorf("missing %s: tenant limits must live in the default profile (review A1)", k)
		}
	}
}

func TestNoSecretValuesInRender(t *testing.T) {
	d, _ := Engine{}.Render(testInput())
	b, _ := yaml.Marshal(d)
	refs := engine.SecretNames(string(b))
	want := map[string]bool{ProviderPasswordSecret("cabc123def45"): true, DefaultPasswordSecret("cabc123def45"): true, AdminPasswordSecret("cabc123def45"): true}
	seen := map[string]bool{}
	for _, r := range refs {
		if !want[r] {
			t.Errorf("unexpected secret reference %q", r)
		}
		seen[r] = true
	}
	for r := range want {
		if !seen[r] {
			t.Errorf("secret reference not used: %s", r)
		}
	}
	secret := find(t, d, "Secret")
	for k, v := range secret["stringData"].(map[string]any) {
		if !strings.HasPrefix(v.(string), "{{secret:") {
			t.Errorf("Secret key %s holds a literal value", k)
		}
	}
}

func TestCertCoversEveryDialledName(t *testing.T) {
	d, _ := Engine{}.Render(testInput())
	names := map[string]bool{}
	for _, n := range d.Certificates[0].DNSNames {
		names[n] = true
	}
	for _, h := range append(d.SQL.Hosts, "cabc123def45.cell1.db.example.com", "keeper-cabc123def45", "chi-cabc123def45-default-0-1",
		"chk-cabc123def45-keeper-0-2.ch-cabc123def45.svc.cluster.local") {
		if !names[h] {
			t.Errorf("certificate is missing %s", h)
		}
	}
}

func TestValidate(t *testing.T) {
	cell := model.Cell{ID: "cell1", Profile: "cloud", Domain: "x"}
	spec := testInput().Cluster.Spec
	if errs := (Engine{}).Validate(spec, cell); len(errs) == 0 {
		t.Error("t-2 must be rejected outside local cells")
	}
	spec.Size = "nope"
	if errs := (Engine{}).Validate(spec, cell); len(errs) == 0 || errs[0].Field != "size" {
		t.Errorf("unknown size not rejected: %v", errs)
	}
	ok := testInput().Cluster.Spec
	if errs := (Engine{}).Validate(ok, testInput().Cell); len(errs) != 0 {
		t.Errorf("valid spec rejected: %v", errs)
	}
	ok.ReplicasPerShard = 3
	if errs := (Engine{}).Validate(ok, testInput().Cell); len(errs) == 0 {
		t.Error("replica count must match the size")
	}
}

func TestObserve(t *testing.T) {
	d, _ := Engine{}.Render(testInput())
	if h := (Engine{}).Observe(d, 3, nil); h.Phase != engine.PhaseProgressing {
		t.Fatalf("nil report: %v", h)
	}
	good := &engine.Observed{
		AppliedGeneration: 3, AppliedHash: "h", BootstrapHash: "h", NamespaceExists: true,
		Objects:   map[string]string{"ClickHouseKeeperInstallation/cabc123def45": "Completed", "ClickHouseInstallation/cabc123def45": "Completed"},
		PodsReady: 5, PodsTotal: 5,
		Probes: []engine.HostProbe{{Host: "a", OK: true, ClusterHosts: 2, KeeperOK: true}, {Host: "b", OK: true, ClusterHosts: 2, KeeperOK: true}},
	}
	if h := (Engine{}).Observe(d, 3, good); h.Phase != engine.PhaseReady {
		t.Fatalf("healthy report not ready: %v", h)
	}
	stale := *good
	stale.AppliedGeneration = 2
	if h := (Engine{}).Observe(d, 3, &stale); h.Phase != engine.PhaseProgressing {
		t.Fatalf("stale generation must not be ready: %v", h)
	}
	// operator says Completed but one replica does not yet see its peer (seen on kind)
	lag := *good
	lag.Probes = []engine.HostProbe{{Host: "a", OK: true, ClusterHosts: 1, KeeperOK: true}, {Host: "b", OK: true, ClusterHosts: 2, KeeperOK: true}}
	if h := (Engine{}).Observe(d, 3, &lag); h.Phase == engine.PhaseReady {
		t.Fatalf("incomplete remote_servers must not be ready: %v", h)
	}
	down := *good
	down.Probes = []engine.HostProbe{{Host: "a", OK: false, Error: "refused"}, {Host: "b", OK: true, ClusterHosts: 2, KeeperOK: true}}
	if h := (Engine{}).Observe(d, 3, &down); h.Phase != engine.PhaseDegraded {
		t.Fatalf("one replica down should be degraded: %v", h)
	}
}

func TestResolveSecrets(t *testing.T) {
	s := "a=" + engine.SecretRef("x/y") + " b=" + engine.SecretSHA256Ref("x/y")
	out, err := engine.ResolveSecrets(s, func(string) (string, error) { return "pw", nil })
	if err != nil {
		t.Fatal(err)
	}
	if out != "a=pw b=30c952fab122c3f9759f02a6d95c3758b246b4fee239957b2d4fee46e26170c4" {
		t.Fatalf("got %q", out)
	}
}
