package agent

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
)

func freeNode(name, zone string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{labelPool: poolFree, labelZone: zone}},
		Spec:       corev1.NodeSpec{Taints: []corev1.Taint{{Key: labelPool, Value: poolFree, Effect: corev1.TaintEffectNoSchedule}}},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	}
}

func req(id string, nodes int) engine.NodePoolRequest {
	return engine.NodePoolRequest{Name: "dedicated-" + id, Nodes: nodes,
		Labels: map[string]string{labelPool: "dedicated", labelDedicatedCluster: id}, TaintKey: labelDedicatedCluster, TaintValue: id}
}

func TestKindNodePoolsClaimSpreadAndRelease(t *testing.T) {
	ctx := context.Background()
	kc := fake.NewSimpleClientset(freeNode("a1", "zone-a"), freeNode("a2", "zone-a"), freeNode("b1", "zone-b"), freeNode("c1", "zone-c"))
	np := KindNodePools{Kube: kc}

	ready, err := np.Ensure(ctx, "c1", req("c1", 3))
	if err != nil || ready != 3 {
		t.Fatalf("ensure: ready=%d err=%v", ready, err)
	}
	zones := map[string]bool{}
	nodes, _ := np.owned(ctx, "c1")
	for _, n := range nodes {
		zones[n.Labels[labelZone]] = true
		if n.Labels[labelPool] != "dedicated" || len(n.Spec.Taints) != 1 || n.Spec.Taints[0].Key != labelDedicatedCluster || n.Spec.Taints[0].Value != "c1" {
			t.Fatalf("node %s not converted correctly: labels=%v taints=%v", n.Name, n.Labels, n.Spec.Taints)
		}
	}
	if len(zones) != 3 {
		t.Fatalf("pool must spread over 3 zones, got %v", zones)
	}
	// idempotent
	if ready, err := np.Ensure(ctx, "c1", req("c1", 3)); err != nil || ready != 3 {
		t.Fatalf("second ensure: %d %v", ready, err)
	}
	// a second cluster cannot get more nodes than are free
	if _, err := np.Ensure(ctx, "c2", req("c2", 2)); err == nil {
		t.Fatal("expected an error when the cell runs out of free nodes")
	}

	// a tenant pod still on a node blocks its release; DaemonSet pods do not
	tenant := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ch-c1"}, Spec: corev1.PodSpec{NodeName: nodes[0].Name}}
	ds := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ds", Namespace: "kube-system", OwnerReferences: []metav1.OwnerReference{{Kind: "DaemonSet", Name: "x"}}},
		Spec: corev1.PodSpec{NodeName: nodes[1].Name}}
	kc.CoreV1().Pods("ch-c1").Create(ctx, tenant, metav1.CreateOptions{})
	kc.CoreV1().Pods("kube-system").Create(ctx, ds, metav1.CreateOptions{})
	// the fake client ignores field selectors, so check hasWorkload directly per node
	if busy, _ := np.hasWorkloadOn(ctx, nodes[0].Name, []corev1.Pod{*tenant, *ds}); !busy {
		t.Fatal("tenant pod must block release")
	}
	if busy, _ := np.hasWorkloadOn(ctx, nodes[1].Name, []corev1.Pod{*tenant, *ds}); busy {
		t.Fatal("DaemonSet pods must not block release")
	}
	kc.CoreV1().Pods("ch-c1").Delete(ctx, "p", metav1.DeleteOptions{})
	kc.CoreV1().Pods("kube-system").Delete(ctx, "ds", metav1.DeleteOptions{})
	if remaining, err := np.Release(ctx, "c1"); err != nil || remaining != 0 {
		t.Fatalf("release: remaining=%d err=%v", remaining, err)
	}
	all, _ := kc.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	for _, n := range all.Items {
		if n.Labels[labelPool] != poolFree || n.Labels[labelDedicatedCluster] != "" || len(n.Spec.Taints) != 1 || n.Spec.Taints[0].Value != poolFree {
			t.Fatalf("node %s not returned to the free pool: %v %v", n.Name, n.Labels, n.Spec.Taints)
		}
	}
}
