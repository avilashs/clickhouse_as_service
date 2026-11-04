package clickhouse

import (
	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

// tableEngines a tenant may use (ADR-0007). With access_control_improvements.
// table_engines_require_grant = true, everything else, including plain MergeTree and
// Executable, is rejected with ACCESS_DENIED (tested on 26.8.34).
var tableEngines = []string{
	"ReplicatedMergeTree", "ReplicatedReplacingMergeTree", "ReplicatedAggregatingMergeTree",
	"ReplicatedSummingMergeTree", "ReplicatedCollapsingMergeTree", "ReplicatedVersionedCollapsingMergeTree",
	"Distributed", "MaterializedView", "View", "Null", "Memory", "Set", "Join", "Buffer", "Dictionary", "Merge",
}

// bootstrapSQL creates the customer_admin role (design §9.3) and the tenant's initial admin
// user in replicated access storage. Every statement is idempotent; the agent re-runs the
// whole list whenever the desired payload changes. Each statement was verified on 26.8.34.
func bootstrapSQL(clusterID string, size model.Size, storageGiB int) []string {
	engines := ""
	for i, e := range tableEngines {
		if i > 0 {
			engines += ", "
		}
		engines += "TABLE ENGINE ON " + e
	}
	return append([]string{
		"CREATE ROLE IF NOT EXISTS customer_admin",
		"GRANT SELECT, INSERT, ALTER, CREATE DATABASE, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, CREATE FUNCTION, " +
			"DROP DATABASE, DROP TABLE, DROP VIEW, DROP DICTIONARY, DROP FUNCTION, TRUNCATE, OPTIMIZE, SHOW, dictGet " +
			"ON *.* TO customer_admin WITH GRANT OPTION",
		// KILL QUERY is not per-user: holding it lets you kill anyone's query, the platform's
		// included (M3 finding). Every user can kill its own queries without it. The explicit
		// revoke also removes it from clusters bootstrapped before M3.
		"REVOKE KILL QUERY ON *.* FROM customer_admin",
		// partial revokes: Keeper paths and the provider's canary database
		"REVOKE SELECT ON system.zookeeper FROM customer_admin",
		"REVOKE SELECT ON system.zookeeper_connection FROM customer_admin",
		"REVOKE SELECT ON system.zookeeper_log FROM customer_admin",
		"REVOKE ALL ON _basalt.* FROM customer_admin",
		// system tables are read-only to tenants: they underpin monitoring and the provider's
		// row policies (M3: without this, ALTER/DROP/TRUNCATE ON *.* reached system.query_log)
		"REVOKE INSERT, ALTER, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP TABLE, DROP VIEW, DROP DICTIONARY, " +
			"TRUNCATE, OPTIMIZE ON system.* FROM customer_admin",
		"GRANT CLUSTER ON *.* TO customer_admin",
		"GRANT " + engines + " TO customer_admin WITH GRANT OPTION",
		"GRANT CREATE USER, ALTER USER, DROP USER, CREATE ROLE, ALTER ROLE, DROP ROLE, CREATE ROW POLICY, ALTER ROW POLICY, " +
			"DROP ROW POLICY, SHOW ACCESS ON *.* TO customer_admin",
		// The platform quota is shared by every tenant user, and quotas cannot be scoped: whoever
		// may ALTER or DROP a quota may remove ours. So tenants do not manage quotas (M4). The
		// REVOKE also covers clusters bootstrapped by M3, which granted them.
		"REVOKE CREATE QUOTA, ALTER QUOTA, DROP QUOTA ON *.* FROM customer_admin",
		quotaSQL(size, storageGiB),
		"REVOKE CREATE ROW POLICY, ALTER ROW POLICY, DROP ROW POLICY ON system.* FROM customer_admin",
		"GRANT SYSTEM FLUSH LOGS, SYSTEM SYNC REPLICA, SYSTEM DROP QUERY CACHE, SYSTEM MERGES, SYSTEM TTL MERGES, " +
			"SYSTEM MOVES, SYSTEM FETCHES, SYSTEM RESTART REPLICA ON *.* TO customer_admin",
		"CREATE USER IF NOT EXISTS " + CustomerAdmin + " IDENTIFIED WITH sha256_hash BY '" +
			engine.SecretSHA256Ref(AdminPasswordSecret(clusterID)) + "'",
		"GRANT customer_admin TO " + CustomerAdmin,
		"ALTER USER " + CustomerAdmin + " DEFAULT ROLE customer_admin",
	}, hideProviderActivity()...)
}

// providerUsers act for the platform; tenants must not see their queries (which include the
// access bootstrap) in the log tables tenants may read.
var providerUsers = []string{ProviderUser, "default", "clickhouse_operator"}

// hideProviderActivity adds restrictive row policies for everyone except the provider user.
// "TO ALL EXCEPT" also covers users the tenant creates later, with or without a role.
func hideProviderActivity() []string {
	in := ""
	for i, u := range providerUsers {
		if i > 0 {
			in += ", "
		}
		in += "'" + u + "'"
	}
	var out []string
	for _, t := range []string{"query_log", "query_thread_log", "query_views_log", "processes", "session_log"} {
		out = append(out, "CREATE ROW POLICY OR REPLACE basalt_hide_provider ON system."+t+
			" AS restrictive FOR SELECT USING user NOT IN ("+in+") TO ALL EXCEPT "+ProviderUser)
	}
	return out
}
