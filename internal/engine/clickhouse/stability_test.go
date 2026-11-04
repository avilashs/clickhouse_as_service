package clickhouse

import (
	"os/exec"
	"sort"
	"strings"
	"testing"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

func chiConfig(t *testing.T, in engine.RenderInput, key string) map[string]any {
	t.Helper()
	d, err := Engine{}.Render(in)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := dig(find(t, d, "ClickHouseInstallation"), "spec", "configuration", key).(map[string]any)
	if !ok {
		t.Fatalf("no spec.configuration.%s", key)
	}
	return m
}

func TestDiskReserve(t *testing.T) {
	for _, c := range []struct {
		storageGiB int
		local      bool
		want       int64
	}{
		{1, false, 128 * mib}, // 5% is 51 MiB: floor
		{5, false, 256 * mib}, // 5%
		{20, false, 1 * gib},  // exactly 5%
		{100, false, 2 * gib}, // 5% is 5 GiB: capped
		{16000, false, 2 * gib},
		{1, true, gib / 20}, // 5% (51 MiB) is inside the local band
		{5, true, 64 * mib}, // 5% is 256 MiB: capped
		{20, true, 64 * mib},
	} {
		if got := DiskReserveBytes(c.storageGiB, c.local); got != c.want {
			t.Errorf("DiskReserveBytes(%d GiB, local=%v) = %d, want %d", c.storageGiB, c.local, got, c.want)
		}
	}
}

func asInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch x := v.(type) {
	case int:
		return int64(x)
	case int64:
		return x
	}
	t.Fatalf("not a number: %v (%T)", v, v)
	return 0
}

func TestStabilitySettingsScaleWithSize(t *testing.T) {
	small := stabilityServerSettings(model.Sizes["t-2"], 5, model.TierShared, false)
	big := stabilityServerSettings(model.Sizes["s-8"], 500, model.TierShared, false)
	for _, k := range []string{
		"background_pool_size", "background_fetches_pool_size", "background_schedule_pool_size",
		"max_table_num_to_throw", "max_database_num_to_throw", "max_replicated_fetches_network_bandwidth_for_server",
	} {
		if asInt64(t, small[k]) >= asInt64(t, big[k]) {
			t.Errorf("%s does not grow with the size: t-2 %v, s-8 %v", k, small[k], big[k])
		}
	}
	if asInt64(t, small["background_pool_size"]) != 2 {
		t.Errorf("a 1-vCPU server needs a small merge pool, got %v", small["background_pool_size"])
	}
	// the stock defaults assume 16 cores; no size may exceed them
	for name, s := range model.Sizes {
		m := stabilityServerSettings(s, s.DefaultStorage, s.Tier, false)
		if asInt64(t, m["background_pool_size"]) > 16 || asInt64(t, m["background_schedule_pool_size"]) > 512 {
			t.Errorf("%s: pools exceed the stock defaults", name)
		}
	}
	d := stabilityServerSettings(model.Sizes["d-m7g-4xl"], 500, model.TierDedicated, false)
	if _, ok := d["max_replicated_fetches_network_bandwidth_for_server"]; ok {
		t.Error("dedicated nodes are not shaped")
	}
}

func TestEveryLogTableHasTTL(t *testing.T) {
	s := systemLogSettings()
	for _, tbl := range SystemLogTables() {
		v, _ := s[tbl+"/ttl"].(string)
		e, _ := s[tbl+"/engine"].(string) // tables whose engine is fixed upstream carry the TTL in it
		if !strings.HasPrefix(v, "event_date + INTERVAL ") && !strings.Contains(strings.ToLower(e), " ttl ") {
			t.Errorf("%s has no TTL", tbl)
		}
		if v != "" && e != "" {
			t.Errorf("%s sets both ttl and engine, which ClickHouse rejects", tbl)
		}
	}
}

func TestQuotaIsPlatformOwnedAndShared(t *testing.T) {
	sql := quotaSQL(model.Sizes["s-2"], 100)
	if strings.Contains(sql, "KEYED BY") {
		t.Error("a keyed quota would multiply with every user the tenant creates")
	}
	for _, want := range []string{"CREATE QUOTA OR REPLACE " + PlatformQuota, "TO ALL EXCEPT provider_ops, default, clickhouse_operator"} {
		if !strings.Contains(sql, want) {
			t.Errorf("quota lacks %q: %s", want, sql)
		}
	}
	all := strings.Join(bootstrapSQL("cx", model.Sizes["s-2"], 100), "\n")
	if strings.Contains(all, "GRANT CREATE USER") && strings.Contains(strings.SplitN(all, "REVOKE CREATE QUOTA", 2)[0], "ALTER QUOTA, DROP QUOTA, SHOW ACCESS") {
		t.Error("customer_admin is still granted quota management")
	}
	if !strings.Contains(all, "REVOKE CREATE QUOTA, ALTER QUOTA, DROP QUOTA ON *.* FROM customer_admin") {
		t.Error("clusters bootstrapped by M3 keep the grant unless it is revoked explicitly")
	}
}

func TestServerPodWaitsForKeeperAndReservesDisk(t *testing.T) {
	d, _ := Engine{}.Render(testInput())
	tpl := dig(find(t, d, "ClickHouseInstallation"), "spec", "templates", "podTemplates").([]any)[0]
	inits, _ := dig(tpl, "spec", "initContainers").([]any)
	if len(inits) != 2 {
		t.Fatalf("want wait-for-keeper and balloon init containers, got %d", len(inits))
	}
	byName := map[string]string{}
	for _, c := range inits {
		m := c.(map[string]any)
		byName[m["name"].(string)] = m["command"].([]any)[2].(string)
		res := m["resources"].(map[string]any)
		if res["requests"].(map[string]any)["memory"] != res["limits"].(map[string]any)["memory"] {
			t.Errorf("%s: memory requests must equal limits", m["name"])
		}
	}
	if !strings.Contains(byName["wait-for-keeper"], "keeper-cabc123def45/2281") {
		t.Errorf("wait-for-keeper does not dial the Keeper client Service: %s", byName["wait-for-keeper"])
	}
	if !strings.Contains(byName["balloon"], "want=67108864") { // 64 MiB: the test cell is local
		t.Errorf("balloon size: %s", byName["balloon"])
	}
	p := chiConfig(t, testInput(), "settings")
	if p["storage_configuration/disks/default/keep_free_space_bytes"] != DiskReserveBytes(5, true) {
		t.Error("keep_free_space_bytes must equal the balloon size")
	}
}

// TestRenderedSettingNamesExistInTarget guards F-23 (a bad rendered config): every setting the
// render emits must be a real name in the pinned ClickHouse version. Server config keys that are
// not in system.server_settings are accepted only under a section listed in knownSections, so a
// typo in a new key fails here instead of being ignored by the server. Needs the clickhouse
// binary (skipped without it); CI installs the pinned release.
func TestRenderedSettingNamesExistInTarget(t *testing.T) {
	bin, err := exec.LookPath("clickhouse")
	if err != nil {
		t.Skip("no clickhouse binary on PATH")
	}
	names := func(q string) map[string]bool {
		out, err := exec.Command(bin, "local", "-q", q).Output()
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		m := map[string]bool{}
		for _, l := range strings.Fields(string(out)) {
			m[l] = true
		}
		return m
	}
	version := strings.TrimSpace(string(must(exec.Command(bin, "local", "-q", "select version()").Output())))
	if !strings.HasPrefix(version, testInput().Cluster.Spec.ServerVersion[:4]) {
		t.Skipf("clickhouse binary is %s, the render targets %s", version, testInput().Cluster.Spec.ServerVersion)
	}
	server := names("select name from system.server_settings")
	queryLevel := names("select name from system.settings")
	mergeTree := names("select name from system.merge_tree_settings")
	// config sections that are not individual server settings
	knownSections := map[string]bool{
		"merge_tree": true, "storage_configuration": true, "remote_url_allow_hosts": true,
		"access_control_improvements": true, "user_directories": true, "openSSL": true,
		"query_log": true, "query_views_log": true, "part_log": true,
		"metric_log": true, "asynchronous_metric_log": true, "text_log": true, "trace_log": true,
		"processors_profile_log": true, "crash_log": true, "error_log": true, "asynchronous_insert_log": true,
		"background_schedule_pool_log": true, "backup_log": true, "blob_storage_log": true,
		"aggregated_zookeeper_log": true, "zookeeper_connection_log": true, "opentelemetry_span_log": true,
		"query_metric_log": true, "delta_lake_metadata_log": true, "iceberg_metadata_log": true,
		"s3queue_log": true, "session_log": true,
		// plain config elements
		"listen_host": true, "http_port": true, "tcp_port": true, "interserver_http_port": true,
		"mysql_port": true, "postgresql_port": true, "https_port": true, "tcp_port_secure": true,
		"interserver_https_port": true, "user_scripts_path": true, "skip_binary_checksum_checks": true,
		"logger": true,
	}
	var bad []string
	for _, in := range []engine.RenderInput{testInput(), dedicatedInput()} {
		for k := range chiConfig(t, in, "settings") {
			first := strings.SplitN(k, "/", 2)
			switch {
			case first[0] == "merge_tree":
				if !mergeTree[strings.SplitN(first[1], "/", 2)[0]] {
					bad = append(bad, "merge_tree setting "+k)
				}
			case server[first[0]] || knownSections[first[0]]:
			default:
				bad = append(bad, "server setting "+k)
			}
		}
		for k := range chiConfig(t, in, "profiles") {
			parts := strings.Split(k, "/")
			name := parts[1]
			if name == "constraints" {
				name = parts[2]
			}
			if !queryLevel[name] {
				bad = append(bad, "profile setting "+k)
			}
		}
	}
	sort.Strings(bad)
	if len(bad) > 0 {
		t.Fatalf("not a setting of ClickHouse %s:\n  %s", version, strings.Join(bad, "\n  "))
	}
}

func must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}
