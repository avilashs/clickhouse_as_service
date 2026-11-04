// basalt-agent runs inside a cell. It pulls desired state from the control plane (outbound
// only), converges the cell and reports observed state.
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

	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/avilashs/clickhouse_as_service/internal/agent"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cell, url, token := os.Getenv("BASALT_CELL_ID"), os.Getenv("BASALT_CONTROL_PLANE_URL"), os.Getenv("BASALT_CELL_AGENT_TOKEN")
	if cell == "" || url == "" || token == "" {
		return errors.New("BASALT_CELL_ID, BASALT_CONTROL_PLANE_URL and BASALT_CELL_AGENT_TOKEN are required")
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
		if err != nil {
			return err
		}
	}
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	dc, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(kc.Discovery()))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	a := &agent.Agent{
		CellID: cell, ControlPlaneURL: url, Token: token, Kube: kc, Dynamic: dc, Mapper: mapper,
		HTTP: &http.Client{Timeout: 30 * time.Second}, Log: log, Interval: 5 * time.Second,
	}
	log.Info("agent started", "cell", cell, "controlPlane", url)
	return a.Run(ctx)
}
