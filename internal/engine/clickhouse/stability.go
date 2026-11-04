package clickhouse

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/avilashs/clickhouse_as_service/internal/model"
)

// Noisy-neighbour and stability controls (design §10, M4). Everything here is derived from the
// size and the requested storage, so renders stay deterministic.

const (
	mib = int64(1) << 20
	gib = int64(1) << 30
)

func vcpus(size model.Size) int {
	v := int(size.MilliCPU / 1000)
	if v < 1 {
		v = 1
	}
	return v
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// DiskReserveBytes is how much of the data volume is held back: ClickHouse refuses writes once
// free space falls below it (keep_free_space_bytes), and a balloon file of the same size can be
// deleted in an emergency. 5% of the volume, between 128 MiB and 2 GiB. Local (kind) cells keep
// 16 to 64 MiB: every replica's volume is a directory on one shared host disk, and 256 MiB
// balloons for each replica filled the M4 test VM.
func DiskReserveBytes(storageGiB int, local bool) int64 {
	lo, hi := 128*mib, 2*gib
	if local {
		lo, hi = 16*mib, 64*mib
	}
	r := int64(storageGiB) * gib / 20
	if r < lo {
		r = lo
	}
	if r > hi {
		r = hi
	}
	return r
}

// BalloonPath is the emergency-release file on the data volume (runbook disk-full).
const BalloonPath = "/var/lib/clickhouse/.basalt-balloon"

// systemLogTTLDays lists every system log table the 26.8 stock config enables. The stock
// config gives most of them no TTL, so they would grow without bound. A test boots a stock
// server with these settings and fails if a log table is left without a TTL (new tables in a
// release fail CI), and it fails if this list names a table the server does not create:
// naming a log that the stock config leaves disabled would switch it on.
var systemLogTTLDays = map[string]int{
	"query_log": 14, "query_views_log": 7, "part_log": 7,
	"metric_log": 3, "asynchronous_metric_log": 3, "text_log": 3, "trace_log": 3,
	"processors_profile_log": 3, "crash_log": 30, "error_log": 7, "asynchronous_insert_log": 7,
	"background_schedule_pool_log": 3, "backup_log": 30, "blob_storage_log": 7,
	"aggregated_zookeeper_log": 7, "zookeeper_connection_log": 7, "opentelemetry_span_log": 3,
	"query_metric_log": 3, "delta_lake_metadata_log": 3, "iceberg_metadata_log": 3, "s3queue_log": 3,
	// not in the stock config; enabled below so that tenant logins are auditable
	"session_log": 30,
}

// SystemLogTables returns the log tables that get a TTL, sorted.
func SystemLogTables() []string {
	out := make([]string, 0, len(systemLogTTLDays))
	for t := range systemLogTTLDays {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// logsWithFixedEngine have an <engine> in the config that precedes ours (ClickHouse rejects a
// separate ttl for them), so the TTL goes inside the engine. query_log, part_log and trace_log
// get theirs from the Altinity operator (a 30-day TTL, and it removes query_thread_log);
// opentelemetry_span_log from the stock config, with a different date column.
var logsWithFixedEngine = map[string]string{
	"query_log":              "Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + interval %d day",
	"part_log":               "Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + interval %d day",
	"trace_log":              "Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + interval %d day",
	"opentelemetry_span_log": "engine MergeTree partition by toYYYYMM(finish_date) order by (finish_date, finish_time_us) TTL finish_date + interval %d day",
}

func systemLogSettings() map[string]any {
	s := map[string]any{}
	for t, d := range systemLogTTLDays {
		if e, ok := logsWithFixedEngine[t]; ok {
			s[t+"/engine"] = fmt.Sprintf(e, d)
			continue
		}
		s[t+"/ttl"] = fmt.Sprintf("event_date + INTERVAL %d DAY", d)
	}
	// session_log is a comment in the stock config, so its stanza is complete here
	s["session_log/database"] = "system"
	s["session_log/table"] = "session_log"
	s["session_log/partition_by"] = "toYYYYMM(event_date)"
	s["session_log/flush_interval_milliseconds"] = 7500
	// the stock level is trace; information keeps text_log useful without flooding it
	s["text_log/level"] = "information"
	return s
}

// stabilityServerSettings are the server-level controls for one size.
func stabilityServerSettings(size model.Size, storageGiB int, tier model.Tier, local bool) map[string]any {
	v := vcpus(size)
	memGiB := int(size.MemoryBytes / gib)
	pool := clamp(2*v, 2, 16)
	tasks := pool * 2 // pool size times background_merges_mutations_concurrency_ratio
	s := map[string]any{
		// background work scales with the vCPUs, not with the node (the stock pools assume 16 cores)
		"background_pool_size":                          pool,
		"background_merges_mutations_concurrency_ratio": 2,
		// The server refuses to start when these exceed the pool's task count (the stock values
		// 8/20/25 assume 32 tasks; M4 found a t-2 server exiting with BAD_ARGUMENTS). They are
		// thresholds on free pool entries, so they must stay below the count to ever be met.
		"merge_tree/number_of_free_entries_in_pool_to_lower_max_size_of_merge":           clamp(tasks/4, 1, 8),
		"merge_tree/number_of_free_entries_in_pool_to_execute_mutation":                  clamp(tasks*5/8, 1, 20),
		"merge_tree/number_of_free_entries_in_pool_to_execute_optimize_entire_partition": clamp(tasks*3/4, 1, 25),
		"background_fetches_pool_size":                                                   clamp(v, 2, 16),
		"background_common_pool_size":                                                    clamp(v, 2, 8),
		"background_move_pool_size":                                                      2,
		"background_schedule_pool_size":                                                  clamp(32*v, 32, 512),
		// queries share the vCPUs instead of each one assuming all of them
		"concurrent_threads_soft_limit_num": 2 * size.MaxThreads,
		// object counts: each replicated table costs memory, Keeper nodes and background tasks
		"max_table_num_to_throw":      clamp(250*memGiB, 500, 10000),
		"max_view_num_to_throw":       clamp(250*memGiB, 500, 10000),
		"max_dictionary_num_to_throw": 200,
		"max_database_num_to_throw":   clamp(50*memGiB, 100, 1000),
		// tenants have no filesystem access, so they cannot create the force_drop flag file
		// that the stock safety limit (50 GB) asks for
		"max_table_size_to_drop":     0,
		"max_partition_size_to_drop": 0,
		// MergeTree guards, pinned so an upgrade cannot change them silently
		"merge_tree/parts_to_delay_insert": 1000,
		"merge_tree/parts_to_throw_insert": 3000,
		"merge_tree/max_delay_to_insert":   1,
		"merge_tree/max_parts_in_total":    100000,
		// disk: stop writing before the volume is full
		"storage_configuration/disks/default/keep_free_space_bytes": DiskReserveBytes(storageGiB, local),
	}
	if tier == model.TierShared {
		// replication traffic shares the node's network; initial values for the M6 benchmark to tune
		bw := int64(32*v) * mib
		s["max_replicated_fetches_network_bandwidth_for_server"] = bw
		s["max_replicated_sends_network_bandwidth_for_server"] = bw
	}
	for k, val := range systemLogSettings() {
		s[k] = val
	}
	return s
}

// stabilityProfile are the default-profile limits (with constraints) that protect the volume
// and the log tables from tenant settings.
func stabilityProfile(storageGiB int) map[string]any {
	vol := int64(storageGiB) * gib
	perQuery := strconv.FormatInt(vol/20, 10) // spill files of one query: 5% of the volume
	perUser := strconv.FormatInt(vol/10, 10)  // all of a user's spill files: 10%
	return map[string]any{
		"default/max_partitions_per_insert_block":                           100,
		"default/constraints/max_partitions_per_insert_block/max":           500,
		"default/max_temporary_data_on_disk_size_for_query":                 perQuery,
		"default/max_temporary_data_on_disk_size_for_user":                  perUser,
		"default/constraints/max_temporary_data_on_disk_size_for_query/max": perQuery,
		"default/constraints/max_temporary_data_on_disk_size_for_user/max":  perUser,
		// the profilers fill trace_log and tenants cannot read symbols anyway (introspection is off)
		"default/query_profiler_real_time_period_ns":                   0,
		"default/query_profiler_cpu_time_period_ns":                    0,
		"default/constraints/query_profiler_real_time_period_ns/const": "",
		"default/constraints/query_profiler_cpu_time_period_ns/const":  "",
		// the platform needs every tenant query in query_log
		"default/log_queries":                   1,
		"default/constraints/log_queries/const": "",
		// The operator's own user (profile clickhouse_operator, log_queries = 0) cannot log in
		// under that constraint: its HTTP requests fail with SETTING_CONSTRAINT_VIOLATION and
		// no server of the cluster is ever seen as alive (M4, live cluster). An explicit
		// constraint in its profile replaces the inherited one.
		"clickhouse_operator/constraints/log_queries/changeable_in_readonly": "",
	}
}

// quotaSQL is the platform-owned quota, shared by every tenant user (no KEYED BY, so creating
// more users does not multiply it). It applies to everyone but the platform's own users and is
// replaced whenever the payload changes. The values are backstops against runaway clients and
// error storms, not billing: legitimate use stays far below them.
func quotaSQL(size model.Size, storageGiB int) string {
	v := int64(vcpus(size))
	conc := int64(size.MaxConcurrent)
	return fmt.Sprintf("CREATE QUOTA OR REPLACE %s FOR INTERVAL 1 hour MAX "+
		"queries = %d, errors = %d, result_rows = %d, read_rows = %d, read_bytes = %d, execution_time = %d, written_bytes = %d "+
		"TO ALL EXCEPT %s, default, clickhouse_operator",
		PlatformQuota,
		conc*3600, conc*600, 5_000_000_000*v, 200_000_000_000*v, 2<<40*v, 4*3600*v,
		int64(storageGiB)*gib*2, ProviderUser)
}

// PlatformQuota is the name of the quota above.
const PlatformQuota = "basalt_default"
