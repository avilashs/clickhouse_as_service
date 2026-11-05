// Package config reads control-plane configuration from the environment.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/avilashs/clickhouse_as_service/internal/model"
	"github.com/avilashs/clickhouse_as_service/internal/store"
)

type Config struct {
	PostgresDSN  string
	TemporalHost string
	SecretsDir   string
	Listen       string
	Tokens       map[string]string // bearer token -> project
	Cell         model.Cell
	CellToken    string
	CellCapacity []store.CellCapacity
	MaxClusters  int
	Backup       BackupConfig
}

// BackupConfig describes the cell's backup storage and the schedules around it. Backups are off
// unless BASALT_BACKUP_URL is set.
type BackupConfig struct {
	// Credentials: a static key (local fixture; stored as secrets, never in the environment of
	// the cell agent) or STS.
	STSEndpoint, STSRoleARN, STSRegion string
	AccessKey, SecretKey               string        // the control plane's own identity for STS and retention
	Interval                           time.Duration // scheduled backups per cluster (0: off)
	FullEvery                          int           // every Nth scheduled backup is a full one
	Retention                          time.Duration // backups older than this are deleted when nothing depends on them
	VerifyInterval                     time.Duration // restore verification per cluster (0: off)
	VerifySize                         string
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return n
		}
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func Load() (Config, error) {
	c := Config{
		PostgresDSN:  env("BASALT_PG_DSN", "postgres://basalt:basalt@127.0.0.1:5432/basalt?sslmode=disable"),
		TemporalHost: env("BASALT_TEMPORAL", "127.0.0.1:7233"),
		SecretsDir:   env("BASALT_SECRETS_DIR", "/var/lib/basalt/secrets"),
		Listen:       env("BASALT_LISTEN", "0.0.0.0:8080"),
		Tokens:       map[string]string{},
		Cell: model.Cell{
			ID: env("BASALT_CELL_ID", "cell1"), Region: env("BASALT_CELL_REGION", "local"), State: "ACTIVE",
			Profile: env("BASALT_CELL_PROFILE", "local"), Domain: env("BASALT_DOMAIN", "db.example.com"),
			Sandbox: env("BASALT_CELL_SANDBOX", "gvisor"),
		},
		CellToken: os.Getenv("BASALT_CELL_AGENT_TOKEN"),
		CellCapacity: []store.CellCapacity{{
			Pool:        "shared",
			MilliCPU:    envInt("BASALT_CELL_SHARED_MILLICPU", 6000),
			MemoryBytes: envInt("BASALT_CELL_SHARED_MEMORY", 12<<30),
			Pods:        int(envInt("BASALT_CELL_SHARED_PODS", 30)),
		}, {
			// dedicated capacity is counted in whole nodes (Pods = nodes)
			Pool: "dedicated", MilliCPU: 0, MemoryBytes: 0,
			Pods: int(envInt("BASALT_CELL_DEDICATED_NODES", 3)),
		}},
		MaxClusters: int(envInt("BASALT_CELL_MAX_CLUSTERS", 50)),
	}
	if u := os.Getenv("BASALT_BACKUP_URL"); u != "" {
		// BASALT_BACKUP_URL is where the servers reach the store: http(s)://host:port/bucket/
		pu, err := url.Parse(u)
		if err != nil || pu.Host == "" || strings.Trim(pu.Path, "/") == "" {
			return c, fmt.Errorf("BASALT_BACKUP_URL must look like http://host:port/bucket/")
		}
		port, _ := strconv.Atoi(pu.Port())
		if port == 0 {
			port = 443
			if pu.Scheme == "http" {
				port = 80
			}
		}
		t := &model.BackupTarget{
			URL: strings.TrimSuffix(u, "/") + "/", Bucket: strings.Trim(pu.Path, "/"), Host: pu.Hostname(), Port: port,
			ControlPlaneEndpoint: env("BASALT_BACKUP_CONTROL_ENDPOINT", pu.Host), Secure: pu.Scheme == "https",
			EgressNamespace: os.Getenv("BASALT_BACKUP_EGRESS_NAMESPACE"),
		}
		if sel := os.Getenv("BASALT_BACKUP_EGRESS_POD_LABELS"); sel != "" {
			t.EgressPodSelector = map[string]string{}
			for _, kv := range strings.Split(sel, ",") {
				k, v, _ := strings.Cut(kv, "=")
				t.EgressPodSelector[k] = v
			}
		}
		if cidrs := os.Getenv("BASALT_BACKUP_EGRESS_CIDRS"); cidrs != "" {
			t.EgressCIDRs = strings.Split(cidrs, ",")
		}
		c.Cell.Backup = t
	}
	c.Backup = BackupConfig{
		STSEndpoint: os.Getenv("BASALT_BACKUP_STS_ENDPOINT"), STSRoleARN: os.Getenv("BASALT_BACKUP_STS_ROLE_ARN"), STSRegion: env("BASALT_BACKUP_STS_REGION", "us-east-1"),
		AccessKey: os.Getenv("BASALT_BACKUP_ACCESS_KEY"), SecretKey: os.Getenv("BASALT_BACKUP_SECRET_KEY"),
		Interval: envDuration("BASALT_BACKUP_INTERVAL", 24*time.Hour), FullEvery: int(envInt("BASALT_BACKUP_FULL_EVERY", 7)),
		Retention: envDuration("BASALT_BACKUP_RETENTION", 14*24*time.Hour), VerifyInterval: envDuration("BASALT_VERIFY_INTERVAL", 24*time.Hour),
		VerifySize: env("BASALT_VERIFY_SIZE", "dev-1"),
	}
	for _, pair := range strings.Split(os.Getenv("BASALT_API_TOKENS"), ",") {
		if pair == "" {
			continue
		}
		project, tok, ok := strings.Cut(pair, ":")
		if !ok || project == "" || len(tok) < 16 {
			return c, fmt.Errorf("BASALT_API_TOKENS entries are project:token with tokens of 16+ chars")
		}
		c.Tokens[tok] = project
	}
	return c, nil
}

func (c Config) CellTokenSHA256() string {
	s := sha256.Sum256([]byte(c.CellToken))
	return hex.EncodeToString(s[:])
}
