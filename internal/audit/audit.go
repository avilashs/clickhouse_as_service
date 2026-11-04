// Package audit checks the placement invariants of design §8 against the actual state of
// the cell (pods and nodes as they are, not as they were rendered or admitted). It backs the
// basalt-audit CLI used by tests and the in-cell service that exports
// basalt_placement_violations for alerting.
package audit

import (
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
)

const (
	labelPool      = "basalt.io/pool"
	labelDedicated = "basalt.io/dedicated-cluster"
	labelComponent = "basalt.io/component"
	labelCluster   = "basalt.io/cluster"
	labelTier      = "basalt.io/tier"
	labelZone      = "topology.kubernetes.io/zone"
	labelChi       = "clickhouse.altinity.com/chi"
	labelShard     = "clickhouse.altinity.com/shard"
)

// Rules.
const (
	RuleShardSameNode    = "V1"  // two replicas of one shard on one node
	RuleShardZoneSpread  = "V1b" // replicas of a shard not spread over the zones they should use
	RuleDedicatedForeign = "V2"  // a pod that is neither that cluster's server nor a DaemonSet on a dedicated node
	RuleDedicatedShared  = "V2b" // more than one ClickHouse server on a dedicated node
	RuleTierPlacement    = "V3"  // shared server without the sandbox or off the shared pool; dedicated server off its pool
	RuleDedicatedCount   = "V4"  // dedicated node count != server count for longer than the grace period
	RuleKeeperPool       = "V5"  // Keeper off the control pool
)

type Violation struct {
	Rule    string `json:"rule"`
	Cluster string `json:"cluster,omitempty"`
	Node    string `json:"node,omitempty"`
	Pod     string `json:"pod,omitempty"`
	Message string `json:"message"`
}

func (v Violation) Key() string { return v.Rule + "|" + v.Cluster + "|" + v.Node + "|" + v.Pod }

type Options struct {
	Sandbox string        // RuntimeClass shared servers must use ("" = not required)
	Grace   time.Duration // how long V4 may persist (node rotations add a surge node)
	Now     time.Time
	// FirstSeen remembers when a V4 mismatch was first observed (service mode); nil = no grace.
	FirstSeen map[string]time.Time
}

func label(m map[string]string, k string) string { return m[k] }

func isDaemonSet(p corev1.Pod) bool {
	for _, o := range p.OwnerReferences {
		if o.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

// infra reports node-level infrastructure allowed on every node: DaemonSet pods, and the
// local-path provisioner's short-lived volume helper pods (local cells).
func infra(p corev1.Pod) bool { return isDaemonSet(p) || p.Namespace == "local-path-storage" }

func active(p corev1.Pod) bool {
	return p.Spec.NodeName != "" && p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed
}

// Check evaluates every rule.
func Check(nodes []corev1.Node, pods []corev1.Pod, o Options) []Violation {
	byName := map[string]corev1.Node{}
	for _, n := range nodes {
		byName[n.Name] = n
	}
	var out []Violation
	add := func(v Violation) { out = append(out, v) }

	type shardKey struct{ chi, shard string }
	shardPods := map[shardKey][]corev1.Pod{}
	serversByCluster := map[string]int{}
	serversOnNode := map[string][]corev1.Pod{}

	for _, p := range pods {
		if !active(p) {
			continue
		}
		node := byName[p.Spec.NodeName]
		comp := label(p.Labels, labelComponent)
		cluster := label(p.Labels, labelCluster)
		switch comp {
		case "clickhouse-server":
			shardPods[shardKey{label(p.Labels, labelChi), label(p.Labels, labelShard)}] = append(shardPods[shardKey{label(p.Labels, labelChi), label(p.Labels, labelShard)}], p)
			serversOnNode[p.Spec.NodeName] = append(serversOnNode[p.Spec.NodeName], p)
			serversByCluster[cluster]++
			switch label(p.Labels, labelTier) {
			case "shared":
				if o.Sandbox != "" && (p.Spec.RuntimeClassName == nil || *p.Spec.RuntimeClassName != o.Sandbox) {
					add(Violation{Rule: RuleTierPlacement, Cluster: cluster, Node: node.Name, Pod: p.Namespace + "/" + p.Name,
						Message: "shared-tier server is not running in the " + o.Sandbox + " sandbox"})
				}
				if label(node.Labels, labelPool) != "shared" {
					add(Violation{Rule: RuleTierPlacement, Cluster: cluster, Node: node.Name, Pod: p.Namespace + "/" + p.Name,
						Message: fmt.Sprintf("shared-tier server on a %q node", label(node.Labels, labelPool))})
				}
			case "dedicated":
				if label(node.Labels, labelDedicated) != cluster {
					add(Violation{Rule: RuleTierPlacement, Cluster: cluster, Node: node.Name, Pod: p.Namespace + "/" + p.Name,
						Message: "dedicated server is not on its own cluster's node pool"})
				}
			}
		case "keeper":
			if label(node.Labels, labelPool) != "control" {
				add(Violation{Rule: RuleKeeperPool, Cluster: cluster, Node: node.Name, Pod: p.Namespace + "/" + p.Name,
					Message: "Keeper is not on the control pool"})
			}
		}
		// V2: anything foreign on a dedicated node
		if owner := label(node.Labels, labelDedicated); owner != "" && !infra(p) &&
			!(comp == "clickhouse-server" && cluster == owner) {
			add(Violation{Rule: RuleDedicatedForeign, Cluster: owner, Node: node.Name, Pod: p.Namespace + "/" + p.Name,
				Message: "pod is not a ClickHouse server of the cluster that owns this dedicated node"})
		}
	}

	// V2b: exclusivity on dedicated nodes
	for node, ps := range serversOnNode {
		if owner := label(byName[node].Labels, labelDedicated); owner != "" && len(ps) > 1 {
			add(Violation{Rule: RuleDedicatedShared, Cluster: owner, Node: node, Message: fmt.Sprintf("%d ClickHouse servers on one dedicated node", len(ps))})
		}
	}

	// zones available per pool, for V1b
	zonesOf := func(match func(corev1.Node) bool) int {
		z := map[string]bool{}
		for _, n := range nodes {
			if match(n) && label(n.Labels, labelZone) != "" {
				z[label(n.Labels, labelZone)] = true
			}
		}
		return len(z)
	}
	sharedZones := zonesOf(func(n corev1.Node) bool { return label(n.Labels, labelPool) == "shared" })

	for k, ps := range shardPods {
		cluster := label(ps[0].Labels, labelCluster)
		// V1
		nodesSeen := map[string]string{}
		for _, p := range ps {
			if other, dup := nodesSeen[p.Spec.NodeName]; dup {
				add(Violation{Rule: RuleShardSameNode, Cluster: cluster, Node: p.Spec.NodeName,
					Message: fmt.Sprintf("replicas %s and %s of shard %s share a node", other, p.Name, k.shard)})
			}
			nodesSeen[p.Spec.NodeName] = p.Name
		}
		// V1b: an HA shard must use min(replicas, 3, zones available) distinct zones
		if len(ps) < 2 {
			continue
		}
		avail := sharedZones
		if label(ps[0].Labels, labelTier) == "dedicated" {
			avail = zonesOf(func(n corev1.Node) bool { return label(n.Labels, labelDedicated) == cluster })
		}
		want := min(len(ps), 3, avail)
		zones := map[string]bool{}
		for _, p := range ps {
			zones[label(byName[p.Spec.NodeName].Labels, labelZone)] = true
		}
		if len(zones) < want {
			add(Violation{Rule: RuleShardZoneSpread, Cluster: cluster,
				Message: fmt.Sprintf("shard %s: %d replicas in %d zones, want %d", k.shard, len(ps), len(zones), want)})
		}
	}

	// V4: node count = server count for dedicated clusters
	poolNodes := map[string]int{}
	for _, n := range nodes {
		if c := label(n.Labels, labelDedicated); c != "" {
			poolNodes[c]++
		}
	}
	for c, n := range poolNodes {
		if n == serversByCluster[c] {
			if o.FirstSeen != nil {
				delete(o.FirstSeen, "V4|"+c)
			}
			continue
		}
		key := "V4|" + c
		since := o.Now
		if o.FirstSeen != nil {
			if t, ok := o.FirstSeen[key]; ok {
				since = t
			} else {
				o.FirstSeen[key] = o.Now
			}
		}
		if o.Now.Sub(since) >= o.Grace {
			add(Violation{Rule: RuleDedicatedCount, Cluster: c,
				Message: fmt.Sprintf("dedicated pool has %d nodes for %d servers", n, serversByCluster[c])})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out
}
