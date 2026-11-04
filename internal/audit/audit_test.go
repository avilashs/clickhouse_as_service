package audit

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func node(name, pool, zone, dedicated string) corev1.Node {
	l := map[string]string{labelPool: pool, labelZone: zone}
	if dedicated != "" {
		l[labelDedicated] = dedicated
	}
	return corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: l}}
}

func server(name, cluster, tier, shard, nodeName string, sandboxed bool) corev1.Pod {
	p := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ch-" + cluster, Labels: map[string]string{
			labelComponent: "clickhouse-server", labelCluster: cluster, labelTier: tier, labelChi: cluster, labelShard: shard}},
		Spec:   corev1.PodSpec{NodeName: nodeName},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	if sandboxed {
		rc := "gvisor"
		p.Spec.RuntimeClassName = &rc
	}
	return p
}

func cell() []corev1.Node {
	return []corev1.Node{
		node("c1", "control", "a", ""), node("c2", "control", "b", ""), node("c3", "control", "c", ""),
		node("s1", "shared", "a", ""), node("s2", "shared", "b", ""), node("s3", "shared", "c", ""),
		node("d1", "dedicated", "a", "dx"), node("d2", "dedicated", "b", "dx"),
	}
}

func healthy() []corev1.Pod {
	return []corev1.Pod{
		server("sa-0", "sa", "shared", "0", "s1", true), server("sa-1", "sa", "shared", "0", "s2", true),
		server("dx-0", "dx", "dedicated", "0", "d1", false), server("dx-1", "dx", "dedicated", "0", "d2", false),
	}
}

func rules(vs []Violation) map[string]int {
	m := map[string]int{}
	for _, v := range vs {
		m[v.Rule]++
	}
	return m
}

func opts() Options { return Options{Sandbox: "gvisor", Now: time.Now()} }

func TestHealthyCellHasNoViolations(t *testing.T) {
	if vs := Check(cell(), healthy(), opts()); len(vs) != 0 {
		t.Fatalf("unexpected violations: %+v", vs)
	}
}

func TestEachRule(t *testing.T) {
	ds := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "kindnet", Namespace: "kube-system",
		OwnerReferences: []metav1.OwnerReference{{Kind: "DaemonSet", Name: "kindnet"}}}, Spec: corev1.PodSpec{NodeName: "d1"}}
	foreign := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "debug", Namespace: "kube-system"}, Spec: corev1.PodSpec{NodeName: "d1"}}
	keeperOff := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "ch-sa", Labels: map[string]string{labelComponent: "keeper", labelCluster: "sa"}},
		Spec: corev1.PodSpec{NodeName: "s3"}}

	cases := []struct {
		name  string
		nodes []corev1.Node
		pods  []corev1.Pod
		want  string
	}{
		{"V1 replicas share a node", cell(), []corev1.Pod{server("a", "sa", "shared", "0", "s1", true), server("b", "sa", "shared", "0", "s1", true)}, RuleShardSameNode},
		{"V1b replicas share a zone", append(cell(), node("s4", "shared", "a", "")),
			[]corev1.Pod{server("a", "sa", "shared", "0", "s1", true), server("b", "sa", "shared", "0", "s4", true)}, RuleShardZoneSpread},
		{"V2 foreign pod on a dedicated node", cell(), append(healthy(), foreign), RuleDedicatedForeign},
		{"V2 another cluster's server on a dedicated node", cell(), append(healthy(), server("x", "other", "dedicated", "0", "d1", false)), RuleDedicatedForeign},
		{"V2b two servers on a dedicated node", cell(), []corev1.Pod{server("dx-0", "dx", "dedicated", "0", "d1", false), server("dx-1", "dx", "dedicated", "1", "d1", false)}, RuleDedicatedShared},
		{"V3 shared server without sandbox", cell(), []corev1.Pod{server("a", "sa", "shared", "0", "s1", false), server("b", "sa", "shared", "0", "s2", true)}, RuleTierPlacement},
		{"V3 shared server on a control node", cell(), []corev1.Pod{server("a", "sa", "shared", "0", "c1", true), server("b", "sa", "shared", "0", "s2", true)}, RuleTierPlacement},
		{"V4 pool larger than the cluster", append(cell(), node("d3", "dedicated", "c", "dx")), healthy(), RuleDedicatedCount},
		{"V5 Keeper off the control pool", cell(), append(healthy(), keeperOff), RuleKeeperPool},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := rules(Check(c.nodes, c.pods, opts()))
			if got[c.want] == 0 {
				t.Fatalf("want %s, got %v", c.want, got)
			}
		})
	}
	// DaemonSet pods are allowed on dedicated nodes
	if vs := Check(cell(), append(healthy(), ds), opts()); len(vs) != 0 {
		t.Fatalf("DaemonSet pod flagged: %+v", vs)
	}
}

func TestV4Grace(t *testing.T) {
	nodes := append(cell(), node("d3", "dedicated", "c", "dx")) // surge node during a rotation
	seen := map[string]time.Time{}
	t0 := time.Now()
	o := Options{Sandbox: "gvisor", Grace: 30 * time.Minute, Now: t0, FirstSeen: seen}
	if got := rules(Check(nodes, healthy(), o)); got[RuleDedicatedCount] != 0 {
		t.Fatal("V4 must not fire inside the grace period")
	}
	o.Now = t0.Add(31 * time.Minute)
	if got := rules(Check(nodes, healthy(), o)); got[RuleDedicatedCount] != 1 {
		t.Fatal("V4 must fire after the grace period")
	}
	o.Now = t0.Add(32 * time.Minute)
	Check(cell(), healthy(), o) // resolved: the timer resets
	if _, ok := seen["V4|dx"]; ok {
		t.Fatal("resolved mismatch must clear its timer")
	}
}
