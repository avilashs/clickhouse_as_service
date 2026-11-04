// Package config reads control-plane configuration from the environment.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"

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
	CellCapacity store.CellCapacity
	MaxClusters  int
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
		},
		CellToken: os.Getenv("BASALT_CELL_AGENT_TOKEN"),
		CellCapacity: store.CellCapacity{
			Pool:        "shared",
			MilliCPU:    envInt("BASALT_CELL_SHARED_MILLICPU", 6000),
			MemoryBytes: envInt("BASALT_CELL_SHARED_MEMORY", 12<<30),
			Pods:        int(envInt("BASALT_CELL_SHARED_PODS", 30)),
		},
		MaxClusters: int(envInt("BASALT_CELL_MAX_CLUSTERS", 50)),
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
