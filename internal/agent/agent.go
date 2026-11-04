// Package agent is the cell agent: it runs inside each data-plane cell, pulls desired state
// from the control plane over an outbound connection, converges the cell with server-side
// apply, and reports observed state. The control plane never connects into the cell.
//
// Safety rules:
//   - Deletion happens only for an explicit tombstone. A cluster missing from the desired
//     snapshot (control-plane bug, partial response, outage) is left untouched.
//   - If the control plane is unreachable, the agent keeps the cell as it is and keeps
//     probing; running clusters are unaffected (design F-12).
package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/avilashs/clickhouse_as_service/internal/api"
	"github.com/avilashs/clickhouse_as_service/internal/engine"
)

const (
	fieldManager      = "basalt-agent"
	annAppliedHash    = "basalt.io/applied-hash"
	annAppliedGen     = "basalt.io/applied-generation"
	annBootstrapHash  = "basalt.io/bootstrap-hash"
	labelManaged      = "basalt.io/managed"
	labelCluster      = "basalt.io/cluster"
	chiGVRName        = "clickhouseinstallations"
	chkGVRName        = "clickhousekeeperinstallations"
	controlPlaneLimit = 16 << 20
)

var (
	chiGVR = schema.GroupVersionResource{Group: "clickhouse.altinity.com", Version: "v1", Resource: chiGVRName}
	chkGVR = schema.GroupVersionResource{Group: "clickhouse-keeper.altinity.com", Version: "v1", Resource: chkGVRName}
)

type Agent struct {
	CellID          string
	ControlPlaneURL string
	Token           string
	Kube            kubernetes.Interface
	Dynamic         dynamic.Interface
	Mapper          meta.RESTMapper
	HTTP            *http.Client
	NodePools       NodePools // nil: the cell offers no dedicated tier
	Log             *slog.Logger
	Interval        time.Duration

	ca          *CA
	certMu      sync.Mutex
	clientCerts map[string]tls.Certificate
}

// Run loops until ctx is done.
func (a *Agent) Run(ctx context.Context) error {
	ca, err := EnsureCA(ctx, a.Kube, a.CellID)
	if err != nil {
		return fmt.Errorf("cell CA: %w", err)
	}
	a.ca = ca
	var last []api.DesiredEntry
	for {
		desired, err := a.fetchDesired(ctx)
		if err != nil {
			// keep converging and reporting on the last known desired state
			a.Log.Warn("control plane unreachable; keeping current state", "err", err)
		} else {
			last = desired.Clusters
		}
		reports := make([]engine.Observed, 0, len(last))
		for _, d := range last {
			reports = append(reports, a.reconcile(ctx, d))
		}
		if err := a.postObserved(ctx, reports); err != nil {
			a.Log.Warn("report failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(a.Interval):
		}
	}
}

func (a *Agent) do(ctx context.Context, method, path string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(a.ControlPlaneURL, "/")+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+a.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, b)
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(resp.Body, controlPlaneLimit)).Decode(out)
	}
	return nil
}

func (a *Agent) fetchDesired(ctx context.Context) (api.DesiredResponse, error) {
	var r api.DesiredResponse
	err := a.do(ctx, http.MethodGet, "/v1/cells/"+a.CellID+"/desired", nil, &r)
	return r, err
}

func (a *Agent) postObserved(ctx context.Context, reports []engine.Observed) error {
	return a.do(ctx, http.MethodPost, "/v1/cells/"+a.CellID+"/observed",
		api.ObservedRequest{CAPEM: string(a.ca.CertPEM), Clusters: reports}, nil)
}

// reconcile converges one cluster and returns what it observed.
func (a *Agent) reconcile(ctx context.Context, e api.DesiredEntry) engine.Observed {
	obs := engine.Observed{ClusterID: e.ClusterID, Objects: map[string]string{}, ObservedAt: time.Now().UTC()}
	ns := e.Desired.Namespace
	if ns == "" || !strings.HasPrefix(ns, "ch-") {
		obs.Error = "refusing desired state with unexpected namespace " + strconv.Quote(ns)
		return obs
	}
	log := a.Log.With("cluster", e.ClusterID)

	if e.Tombstone {
		gone, err := a.teardown(ctx, e.ClusterID, ns)
		if err != nil {
			obs.Error = err.Error()
			log.Warn("teardown", "err", err)
		}
		obs.NamespaceExists = !gone
		if gone && a.NodePools != nil {
			// nodes go back to the cell only after the tenant's pods are gone
			remaining, err := a.NodePools.Release(ctx, e.ClusterID)
			if err != nil {
				obs.Error = "node pool release: " + err.Error()
			}
			obs.PoolNodes = remaining
		}
		obs.AppliedGeneration = e.Generation
		obs.AppliedHash = e.Hash
		return obs
	}

	nsObj, err := a.Kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	applied := ""
	if err == nil {
		applied = nsObj.Annotations[annAppliedHash]
	}
	if len(e.Desired.NodePools) > 0 {
		if a.NodePools == nil {
			obs.Error = "this cell has no node-pool provider (dedicated tier unavailable)"
			return obs
		}
		for _, np := range e.Desired.NodePools {
			ready, err := a.NodePools.Ensure(ctx, e.ClusterID, np)
			obs.PoolNodes += ready
			if err != nil {
				obs.Error = "node pool: " + err.Error()
				log.Warn("node pool", "err", err)
			}
		}
	}
	if applied != e.Hash {
		if err := a.apply(ctx, e); err != nil {
			obs.Error = "apply: " + err.Error()
			log.Warn("apply failed", "err", err)
		} else {
			log.Info("applied", "generation", e.Generation)
		}
	} else {
		// the payload is current, but certificates may still need renewal
		for _, cr := range e.Desired.Certificates {
			if err := a.ca.EnsureCert(ctx, a.Kube, cr, map[string]string{labelCluster: e.ClusterID}); err != nil {
				obs.Error = "certificate: " + err.Error()
			}
		}
	}

	nsObj, err = a.Kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if err != nil {
		if obs.Error == "" {
			obs.Error = "namespace: " + err.Error()
		}
		return obs
	}
	obs.NamespaceExists = true
	obs.AppliedHash = nsObj.Annotations[annAppliedHash]
	obs.AppliedGeneration, _ = strconv.ParseInt(nsObj.Annotations[annAppliedGen], 10, 64)
	obs.BootstrapHash = nsObj.Annotations[annBootstrapHash]

	a.observeObjects(ctx, e, &obs)
	if e.Desired.SQL != nil {
		obs.Probes = a.probe(ctx, e.Desired)
		allOK := len(obs.Probes) == e.Desired.Expect.ServerHosts
		for _, p := range obs.Probes {
			allOK = allOK && p.OK && p.KeeperOK && p.ClusterHosts == e.Desired.Expect.ServerHosts
		}
		// Bootstrap needs every replica up and Keeper reachable, because users and roles live
		// in replicated access storage.
		if allOK && obs.AppliedHash == e.Hash && obs.BootstrapHash != e.Hash {
			if err := a.bootstrap(ctx, e.Desired); err != nil {
				obs.Error = "bootstrap: " + err.Error()
				log.Warn("bootstrap failed", "err", err)
			} else if err := a.annotate(ctx, ns, map[string]string{annBootstrapHash: e.Hash}); err == nil {
				obs.BootstrapHash = e.Hash
				log.Info("bootstrap applied")
			}
		}
	}
	return obs
}

// apply converges the cluster's objects: namespace first, then certificates (pods mount
// them), then everything else in order, all with server-side apply and a forced field
// manager so the agent's intent wins over drift.
func (a *Agent) apply(ctx context.Context, e api.DesiredEntry) error {
	objs := e.Desired.Objects
	if len(objs) == 0 || objs[0]["kind"] != "Namespace" {
		return errors.New("desired objects must start with the Namespace")
	}
	if err := a.ssa(ctx, objs[0]); err != nil {
		return err
	}
	for _, cr := range e.Desired.Certificates {
		if err := a.ca.EnsureCert(ctx, a.Kube, cr, map[string]string{labelCluster: e.ClusterID}); err != nil {
			return err
		}
	}
	for _, o := range objs[1:] {
		md, _ := o["metadata"].(map[string]any)
		if ns, _ := md["namespace"].(string); ns != e.Desired.Namespace {
			return fmt.Errorf("object %v/%v is outside the cluster namespace", o["kind"], md["name"])
		}
		if err := a.ssa(ctx, o); err != nil {
			return fmt.Errorf("%v/%v: %w", o["kind"], md["name"], err)
		}
	}
	return a.annotate(ctx, e.Desired.Namespace, map[string]string{
		annAppliedHash: e.Hash, annAppliedGen: strconv.FormatInt(e.Generation, 10),
	})
}

func (a *Agent) ssa(ctx context.Context, o engine.Object) error {
	u := &unstructured.Unstructured{Object: o}
	gvk := u.GroupVersionKind()
	m, err := a.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return err
	}
	body, err := json.Marshal(o)
	if err != nil {
		return err
	}
	var ri dynamic.ResourceInterface = a.Dynamic.Resource(m.Resource)
	if m.Scope.Name() == meta.RESTScopeNameNamespace {
		ri = a.Dynamic.Resource(m.Resource).Namespace(u.GetNamespace())
	}
	force := true
	_, err = ri.Patch(ctx, u.GetName(), types.ApplyPatchType, body, metav1.PatchOptions{FieldManager: fieldManager, Force: &force})
	return err
}

func (a *Agent) annotate(ctx context.Context, ns string, ann map[string]string) error {
	patch, _ := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": ann}})
	_, err := a.Kube.CoreV1().Namespaces().Patch(ctx, ns, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: fieldManager})
	return err
}

// teardown deletes a tombstoned cluster: operator resources first (so the operator runs its
// finalizers while the namespace still exists), then the namespace. It only ever deletes a
// namespace labelled as managed by Basalt for this exact cluster. Returns true when gone.
func (a *Agent) teardown(ctx context.Context, clusterID, ns string) (bool, error) {
	nsObj, err := a.Kube.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if nsObj.Labels[labelManaged] != "true" || nsObj.Labels[labelCluster] != clusterID {
		return false, fmt.Errorf("namespace %s is not labelled as cluster %s; refusing to delete", ns, clusterID)
	}
	for _, gvr := range []schema.GroupVersionResource{chiGVR, chkGVR} {
		list, err := a.Dynamic.Resource(gvr).Namespace(ns).List(ctx, metav1.ListOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		if list != nil && len(list.Items) > 0 {
			for _, it := range list.Items {
				if it.GetDeletionTimestamp() == nil {
					if err := a.Dynamic.Resource(gvr).Namespace(ns).Delete(ctx, it.GetName(), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
						return false, err
					}
				}
			}
			return false, nil // wait for the operator to finish before removing the namespace
		}
	}
	if nsObj.DeletionTimestamp == nil {
		if err := a.Kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		a.Log.Info("deleting namespace", "cluster", clusterID, "namespace", ns)
	}
	return false, nil
}

func (a *Agent) observeObjects(ctx context.Context, e api.DesiredEntry, obs *engine.Observed) {
	ns := e.Desired.Namespace
	for _, o := range e.Desired.Objects {
		kind, _ := o["kind"].(string)
		var gvr schema.GroupVersionResource
		switch kind {
		case "ClickHouseInstallation":
			gvr = chiGVR
		case "ClickHouseKeeperInstallation":
			gvr = chkGVR
		default:
			continue
		}
		name, _ := o["metadata"].(map[string]any)["name"].(string)
		u, err := a.Dynamic.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		key := kind + "/" + name
		if err != nil {
			obs.Objects[key] = "error: " + err.Error()
			continue
		}
		st, _, _ := unstructured.NestedString(u.Object, "status", "status")
		obs.Objects[key] = st
	}
	pods, err := a.Kube.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}
	for _, p := range pods.Items {
		obs.PodsTotal++
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				obs.PodsReady++
			}
		}
	}
}

// clientCert returns a cached client certificate for cn, re-minted a week before expiry.
func (a *Agent) clientCert(cn string) (tls.Certificate, error) {
	a.certMu.Lock()
	defer a.certMu.Unlock()
	if c, ok := a.clientCerts[cn]; ok && time.Until(c.Leaf.NotAfter) > 7*24*time.Hour {
		return c, nil
	}
	c, err := a.ca.ClientCertificate(cn, 30*24*time.Hour)
	if err != nil {
		return c, err
	}
	if a.clientCerts == nil {
		a.clientCerts = map[string]tls.Certificate{}
	}
	a.clientCerts[cn] = c
	return c, nil
}

func (a *Agent) conn(d engine.Desired, host string) (clickhouse.Conn, error) {
	tc := &tls.Config{RootCAs: a.ca.Pool(), ServerName: host, MinVersion: tls.VersionTLS12}
	if d.SQL.ClientCertificateCN != "" {
		c, err := a.clientCert(d.SQL.ClientCertificateCN)
		if err != nil {
			return nil, err
		}
		tc.Certificates = []tls.Certificate{c}
	}
	return clickhouse.Open(&clickhouse.Options{
		Addr:        []string{fmt.Sprintf("%s:%d", host, d.SQL.Port)},
		Auth:        clickhouse.Auth{Database: "default", Username: d.SQL.User, Password: d.SQL.PasswordRef},
		TLS:         tc,
		DialTimeout: 5 * time.Second, ReadTimeout: 20 * time.Second,
	})
}

// probe runs the in-cell SQL health checks against every replica.
func (a *Agent) probe(ctx context.Context, d engine.Desired) []engine.HostProbe {
	out := make([]engine.HostProbe, 0, len(d.SQL.Hosts))
	for _, h := range d.SQL.Hosts {
		p := engine.HostProbe{Host: h}
		func() {
			c, err := a.conn(d, h)
			if err != nil {
				p.Error = err.Error()
				return
			}
			defer c.Close()
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			var n uint64
			if err := c.QueryRow(pctx, "SELECT count() FROM system.clusters WHERE cluster = 'default'").Scan(&n); err != nil {
				p.Error = err.Error()
				return
			}
			p.OK, p.ClusterHosts = true, int(n)
			var k uint64
			p.KeeperOK = c.QueryRow(pctx, "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&k) == nil
		}()
		out = append(out, p)
	}
	return out
}

// bootstrap runs the engine's idempotent access SQL on the first replica; replicated access
// storage propagates it to the others through Keeper.
func (a *Agent) bootstrap(ctx context.Context, d engine.Desired) error {
	c, err := a.conn(d, d.SQL.Hosts[0])
	if err != nil {
		return err
	}
	defer c.Close()
	for i, stmt := range d.Bootstrap {
		bctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := c.Exec(bctx, stmt)
		cancel()
		if err != nil {
			return fmt.Errorf("statement %d: %w", i+1, err)
		}
	}
	return nil
}
