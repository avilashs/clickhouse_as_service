// basalt-audit checks placement invariants (design §8.4) against the live cell.
//
//	basalt-audit --once [-o json|text]   one pass; exit 1 if there are violations (tests, operators)
//	basalt-audit --listen :9102          service: re-check every --interval, export Prometheus metrics
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/avilashs/clickhouse_as_service/internal/audit"
)

func main() {
	once := flag.Bool("once", false, "run one check and exit (1 if violations)")
	output := flag.String("o", "text", "output format for --once: text or json")
	listen := flag.String("listen", ":9102", "metrics address (service mode)")
	interval := flag.Duration("interval", 30*time.Second, "check interval (service mode)")
	grace := flag.Duration("grace", 30*time.Minute, "how long a dedicated node-count mismatch may last (rotations add a surge node)")
	sandbox := flag.String("sandbox", "gvisor", "RuntimeClass shared-tier servers must use")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
	}
	if err != nil {
		log.Error("kubernetes config", "err", err)
		os.Exit(2)
	}
	kc, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Error("kubernetes client", "err", err)
		os.Exit(2)
	}
	check := func(ctx context.Context, o audit.Options) ([]audit.Violation, error) {
		nodes, err := kc.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		pods, err := kc.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		o.Sandbox, o.Now = *sandbox, time.Now()
		return audit.Check(nodes.Items, pods.Items, o), nil
	}

	if *once {
		vs, err := check(context.Background(), audit.Options{Grace: *grace})
		if err != nil {
			log.Error("check", "err", err)
			os.Exit(2)
		}
		if *output == "json" {
			if vs == nil {
				vs = []audit.Violation{}
			}
			_ = json.NewEncoder(os.Stdout).Encode(vs)
		} else {
			for _, v := range vs {
				fmt.Printf("%-4s cluster=%s node=%s pod=%s: %s\n", v.Rule, v.Cluster, v.Node, v.Pod, v.Message)
			}
			fmt.Printf("%d violations\n", len(vs))
		}
		if len(vs) > 0 {
			os.Exit(1)
		}
		return
	}

	var mu sync.Mutex
	var last []audit.Violation
	var lastOK time.Time
	seen := map[string]time.Time{}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		for {
			vs, err := check(ctx, audit.Options{Grace: *grace, FirstSeen: seen})
			mu.Lock()
			if err != nil {
				log.Warn("check failed", "err", err)
			} else {
				last, lastOK = vs, time.Now()
				for _, v := range vs {
					log.Warn("placement violation", "rule", v.Rule, "cluster", v.Cluster, "node", v.Node, "pod", v.Pod, "message", v.Message)
				}
			}
			mu.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-time.After(*interval):
			}
		}
	}()
	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintln(w, "# HELP basalt_placement_violations Placement invariant violations currently observed (design §8.4).")
		fmt.Fprintln(w, "# TYPE basalt_placement_violations gauge")
		for _, v := range last {
			fmt.Fprintf(w, "basalt_placement_violations{rule=%s,cluster=%s,node=%s} 1\n", q(v.Rule), q(v.Cluster), q(v.Node))
		}
		fmt.Fprintln(w, "# HELP basalt_placement_violations_count Number of placement violations in the last check.")
		fmt.Fprintln(w, "# TYPE basalt_placement_violations_count gauge")
		fmt.Fprintf(w, "basalt_placement_violations_count %d\n", len(last))
		fmt.Fprintln(w, "# HELP basalt_placement_audit_last_success_timestamp_seconds Time of the last successful check.")
		fmt.Fprintln(w, "# TYPE basalt_placement_audit_last_success_timestamp_seconds gauge")
		fmt.Fprintf(w, "basalt_placement_audit_last_success_timestamp_seconds %d\n", lastOK.Unix())
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	srv := &http.Server{Addr: *listen, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); _ = srv.Close() }()
	log.Info("audit service", "listen", *listen, "interval", interval.String())
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("listen", "err", err)
		os.Exit(2)
	}
}

func q(s string) string { return strconv.Quote(strings.ReplaceAll(s, "\n", " ")) }
