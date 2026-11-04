// basalt-worker runs the Temporal workflows and activities, plus the pending-operation sweeper.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/avilashs/clickhouse_as_service/internal/config"
	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/engine/clickhouse"
	"github.com/avilashs/clickhouse_as_service/internal/secrets"
	"github.com/avilashs/clickhouse_as_service/internal/store"
	"github.com/avilashs/clickhouse_as_service/internal/workflows"
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
	sec, err := secrets.NewFileStore(cfg.SecretsDir)
	if err != nil {
		return err
	}
	tc, err := client.Dial(client.Options{HostPort: cfg.TemporalHost})
	if err != nil {
		return err
	}
	defer tc.Close()

	acts := &workflows.Activities{Store: st, Secrets: sec, Engines: map[string]engine.Engine{"clickhouse": clickhouse.Engine{}}}
	w := worker.New(tc, workflows.TaskQueue, worker.Options{})
	w.RegisterWorkflow(workflows.CreateCluster)
	w.RegisterWorkflow(workflows.DeleteCluster)
	w.RegisterActivity(acts)

	go sweep(ctx, log, st, tc)
	log.Info("worker started", "taskQueue", workflows.TaskQueue)
	return w.Run(worker.InterruptCh())
}

// sweep starts workflows for operations left PENDING (e.g. the API crashed after committing
// the operation but before starting its workflow). Start is idempotent per cluster.
func sweep(ctx context.Context, log *slog.Logger, st *store.Store, tc client.Client) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		ops, err := st.PendingOperations(ctx, 15*time.Second)
		if err != nil {
			log.Warn("sweep", "err", err)
			continue
		}
		for _, op := range ops {
			if err := workflows.Start(ctx, tc, op); err != nil {
				log.Warn("sweep start", "operation", op.ID, "err", err)
			} else {
				log.Info("sweep started pending operation", "operation", op.ID)
			}
		}
	}
}
