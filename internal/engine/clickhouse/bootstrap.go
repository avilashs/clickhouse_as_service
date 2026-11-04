package clickhouse

import "github.com/avilashs/clickhouse_as_service/internal/engine"

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
func bootstrapSQL(clusterID string) []string {
	engines := ""
	for i, e := range tableEngines {
		if i > 0 {
			engines += ", "
		}
		engines += "TABLE ENGINE ON " + e
	}
	return []string{
		"CREATE ROLE IF NOT EXISTS customer_admin",
		"GRANT SELECT, INSERT, ALTER, CREATE DATABASE, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, CREATE FUNCTION, " +
			"DROP DATABASE, DROP TABLE, DROP VIEW, DROP DICTIONARY, DROP FUNCTION, TRUNCATE, OPTIMIZE, SHOW, KILL QUERY, dictGet " +
			"ON *.* TO customer_admin WITH GRANT OPTION",
		// partial revokes: Keeper paths and the provider's canary database
		"REVOKE SELECT ON system.zookeeper FROM customer_admin",
		"REVOKE SELECT ON system.zookeeper_connection FROM customer_admin",
		"REVOKE SELECT ON system.zookeeper_log FROM customer_admin",
		"REVOKE ALL ON _basalt.* FROM customer_admin",
		"GRANT CLUSTER ON *.* TO customer_admin",
		"GRANT " + engines + " TO customer_admin WITH GRANT OPTION",
		"GRANT CREATE USER, ALTER USER, DROP USER, CREATE ROLE, ALTER ROLE, DROP ROLE, CREATE ROW POLICY, ALTER ROW POLICY, " +
			"DROP ROW POLICY, CREATE QUOTA, ALTER QUOTA, DROP QUOTA, SHOW ACCESS ON *.* TO customer_admin",
		"GRANT SYSTEM FLUSH LOGS, SYSTEM SYNC REPLICA, SYSTEM DROP QUERY CACHE, SYSTEM MERGES, SYSTEM TTL MERGES, " +
			"SYSTEM MOVES, SYSTEM FETCHES, SYSTEM RESTART REPLICA ON *.* TO customer_admin",
		"CREATE USER IF NOT EXISTS " + CustomerAdmin + " IDENTIFIED WITH sha256_hash BY '" +
			engine.SecretSHA256Ref(AdminPasswordSecret(clusterID)) + "'",
		"GRANT customer_admin TO " + CustomerAdmin,
		"ALTER USER " + CustomerAdmin + " DEFAULT ROLE customer_admin",
	}
}
