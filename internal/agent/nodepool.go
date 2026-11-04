package agent

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
)

const (
	labelPool             = "basalt.io/pool"
	labelDedicatedCluster = "basalt.io/dedicated-cluster"
	labelNodePool         = "basalt.io/node-pool"
	labelZone             = "topology.kubernetes.io/zone"
	poolFree              = "free"
)

// NodePools provides per-cluster node pools for the dedicated tier.
type NodePools interface {
	// Ensure makes the pool exist with at least req.Nodes nodes; returns the Ready count.
	Ensure(ctx context.Context, clusterID string, req engine.NodePoolRequest) (int, error)
	// Release returns the cluster's nodes to the cell once no workload pods remain on them;
	// returns how many nodes the cluster still holds.
	Release(ctx context.Context, clusterID string) (int, error)
	// Count returns how many Ready nodes the cluster holds.
	Count(ctx context.Context, clusterID string) (int, error)
}

// KindNodePools is the local-cell provider: it claims pre-created nodes labelled
// basalt.io/pool=free, spreading over zones, and gives them back on release. In a cloud
// cell the same request becomes a Karpenter NodePool with limits = Nodes (design §8.1).
type KindNodePools struct{ Kube kubernetes.Interface }

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (k KindNodePools) owned(ctx context.Context, clusterID string) ([]corev1.Node, error) {
	l, err := k.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: labelDedicatedCluster + "=" + clusterID})
	if err != nil {
		return nil, err
	}
	return l.Items, nil
}

func (k KindNodePools) Count(ctx context.Context, clusterID string) (int, error) {
	nodes, err := k.owned(ctx, clusterID)
	n := 0
	for i := range nodes {
		if nodeReady(&nodes[i]) && !nodes[i].Spec.Unschedulable {
			n++
		}
	}
	return n, err
}

func (k KindNodePools) Ensure(ctx context.Context, clusterID string, req engine.NodePoolRequest) (int, error) {
	have, err := k.owned(ctx, clusterID)
	if err != nil {
		return 0, err
	}
	if need := req.Nodes - len(have); need > 0 {
		free, err := k.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: labelPool + "=" + poolFree})
		if err != nil {
			return 0, err
		}
		zones := map[string]int{}
		for _, n := range have {
			zones[n.Labels[labelZone]]++
		}
		var candidates []corev1.Node
		for _, n := range free.Items {
			if nodeReady(&n) && !n.Spec.Unschedulable {
				candidates = append(candidates, n)
			}
		}
		// all or nothing: a partial claim would strand nodes on a cluster that cannot run
		if len(candidates) < need {
			return len(have), fmt.Errorf("node pool %s needs %d more nodes; only %d free nodes in the cell", req.Name, need, len(candidates))
		}
		for i := 0; i < need; i++ {
			// least-used zone first, so the pool spreads like the pods must (R1b)
			sort.SliceStable(candidates, func(a, b int) bool {
				za, zb := zones[candidates[a].Labels[labelZone]], zones[candidates[b].Labels[labelZone]]
				if za != zb {
					return za < zb
				}
				return candidates[a].Name < candidates[b].Name
			})
			pick := candidates[0]
			candidates = candidates[1:]
			if err := k.claim(ctx, pick.Name, clusterID, req); err != nil {
				return len(have), err
			}
			zones[pick.Labels[labelZone]]++
			have = append(have, pick)
		}
	}
	return k.Count(ctx, clusterID)
}

func setTaint(n *corev1.Node, key, value string, keep func(corev1.Taint) bool) {
	var out []corev1.Taint
	for _, t := range n.Spec.Taints {
		if keep(t) {
			out = append(out, t)
		}
	}
	if key != "" {
		out = append(out, corev1.Taint{Key: key, Value: value, Effect: corev1.TaintEffectNoSchedule})
	}
	n.Spec.Taints = out
}

func basaltTaint(t corev1.Taint) bool { return t.Key == labelPool || t.Key == labelDedicatedCluster }

// claim turns a free node into a node of the cluster's pool. It only touches nodes that are
// still free (the update fails on conflict if another claim raced us).
func (k KindNodePools) claim(ctx context.Context, node, clusterID string, req engine.NodePoolRequest) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		n, err := k.Kube.CoreV1().Nodes().Get(ctx, node, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if n.Labels[labelPool] != poolFree {
			return fmt.Errorf("node %s is no longer free", node)
		}
		for key, v := range req.Labels {
			n.Labels[key] = v
		}
		n.Labels[labelNodePool] = req.Name
		setTaint(n, req.TaintKey, req.TaintValue, func(t corev1.Taint) bool { return !basaltTaint(t) })
		_, err = k.Kube.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{FieldManager: fieldManager})
		return err
	})
}

func (k KindNodePools) Release(ctx context.Context, clusterID string) (int, error) {
	nodes, err := k.owned(ctx, clusterID)
	if err != nil {
		return 0, err
	}
	remaining := 0
	for _, n := range nodes {
		busy, err := k.hasWorkload(ctx, n.Name)
		if err != nil {
			return len(nodes), err
		}
		if busy {
			remaining++
			continue
		}
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			cur, err := k.Kube.CoreV1().Nodes().Get(ctx, n.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if cur.Labels[labelDedicatedCluster] != clusterID {
				return nil
			}
			delete(cur.Labels, labelDedicatedCluster)
			delete(cur.Labels, labelNodePool)
			cur.Labels[labelPool] = poolFree
			setTaint(cur, labelPool, poolFree, func(t corev1.Taint) bool { return !basaltTaint(t) })
			_, err = k.Kube.CoreV1().Nodes().Update(ctx, cur, metav1.UpdateOptions{FieldManager: fieldManager})
			return err
		})
		if err != nil {
			return len(nodes), err
		}
	}
	return remaining, nil
}

// hasWorkload reports whether non-DaemonSet pods still run on the node. A node goes back to
// the free pool only when the previous tenant's pods are gone.
func (k KindNodePools) hasWorkload(ctx context.Context, node string) (bool, error) {
	pods, err := k.Kube.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + node})
	if err != nil {
		return false, err
	}
	return k.hasWorkloadOn(ctx, node, pods.Items)
}

func (KindNodePools) hasWorkloadOn(_ context.Context, node string, pods []corev1.Pod) (bool, error) {
	for _, p := range pods {
		if p.Spec.NodeName != node {
			continue
		}
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		ds := false
		for _, o := range p.OwnerReferences {
			ds = ds || o.Kind == "DaemonSet"
		}
		if !ds {
			return true, nil
		}
	}
	return false, nil
}
