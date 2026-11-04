// basalt-api serves the tenant REST API and the cell-agent API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"

	openapi "github.com/avilashs/clickhouse_as_service/api"
	"github.com/avilashs/clickhouse_as_service/internal/api"
	"github.com/avilashs/clickhouse_as_service/internal/config"
	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/engine/clickhouse"
	"github.com/avilashs/clickhouse_as_service/internal/secrets"
	"github.com/avilashs/clickhouse_as_service/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg.CellToken == "" {
		return errors.New("BASALT_CELL_AGENT_TOKEN is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	if err := st.EnsureCell(ctx, cfg.Cell, cfg.MaxClusters, cfg.CellTokenSHA256(), cfg.CellCapacity); err != nil {
		return err
	}
	sec, err := secrets.NewFileStore(cfg.SecretsDir)
	if err != nil {
		return err
	}
	tc, err := client.Dial(client.Options{HostPort: cfg.TemporalHost, Logger: slogAdapter{log}})
	if err != nil {
		return err
	}
	defer tc.Close()

	srv := &api.Server{
		Store: st, Secrets: sec, Temporal: tc, Tokens: cfg.Tokens, OpenAPI: openapi.Spec, Log: log,
		Engines: map[string]engine.Engine{"clickhouse": clickhouse.Engine{}},
	}
	hs := &http.Server{Addr: cfg.Listen, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = hs.Shutdown(sctx)
	}()
	log.Info("listening", "addr", cfg.Listen)
	if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

type slogAdapter struct{ l *slog.Logger }

func (a slogAdapter) Debug(msg string, kv ...any) {}
func (a slogAdapter) Info(msg string, kv ...any)  {}
func (a slogAdapter) Warn(msg string, kv ...any)  { a.l.Warn(msg, kv...) }
func (a slogAdapter) Error(msg string, kv ...any) { a.l.Error(msg, kv...) }
