package clickhouse

import "github.com/avilashs/clickhouse_as_service/internal/model"

// placement renders the scheduling rules of design §8.2 into the server pod spec.
//
//	R1   replicas of one shard never share a node          (required anti-affinity, per shard)
//	R1b  replicas of one shard spread over 3 zones           (maxSkew 1, minDomains 3)
//	R2   dedicated: one ClickHouse server per node, of any   (required anti-affinity against
//	     cluster in any namespace                             all servers, namespaceSelector {})
//	R2   dedicated: only the cluster's own node pool          (nodeSelector + unique taint)
//	R3   shared: only the shared pool, under the sandbox      (nodeSelector, runtimeClassName)
//
// One pod template serves every shard, so per-shard rules use matchLabelKeys on the
// operator's shard label instead of a hard-coded shard value.
func placement(n names, c model.Cluster, cell model.Cell) map[string]any {
	clusterPods := map[string]any{"matchLabels": map[string]any{chiLabel: n.chi}}
	sameShard := []any{shardLabel}
	zoneSpread := []any{map[string]any{
		"maxSkew": 1, "minDomains": 3, "topologyKey": "topology.kubernetes.io/zone",
		"whenUnsatisfiable": "DoNotSchedule", "labelSelector": clusterPods, "matchLabelKeys": sameShard,
	}}
	shardAntiAffinity := map[string]any{ // R1
		"labelSelector": clusterPods, "matchLabelKeys": sameShard, "topologyKey": "kubernetes.io/hostname",
	}

	if c.Spec.Tier == model.TierDedicated {
		return map[string]any{
			"nodeSelector": map[string]any{LabelPool: "dedicated", LabelDedicatedCluster: n.id},
			"tolerations":  []any{toleration(LabelDedicatedCluster, n.id)},
			"affinity": map[string]any{"podAntiAffinity": map[string]any{
				"requiredDuringSchedulingIgnoredDuringExecution": []any{
					shardAntiAffinity,
					map[string]any{ // R2: exclusive node, against every ClickHouse server in the cell
						"labelSelector":     map[string]any{"matchLabels": map[string]any{LabelComponent: ComponentServer}},
						"namespaceSelector": map[string]any{},
						"topologyKey":       "kubernetes.io/hostname",
					},
				},
			}},
			"topologySpreadConstraints": zoneSpread,
		}
	}

	p := map[string]any{
		"nodeSelector": map[string]any{LabelPool: "shared"},
		"tolerations":  []any{toleration(LabelPool, "shared")},
		"affinity": map[string]any{"podAntiAffinity": map[string]any{
			"requiredDuringSchedulingIgnoredDuringExecution": []any{shardAntiAffinity},
			// "never two pods of one cluster on a node where avoidable"
			"preferredDuringSchedulingIgnoredDuringExecution": []any{map[string]any{
				"weight":          100,
				"podAffinityTerm": map[string]any{"labelSelector": clusterPods, "topologyKey": "kubernetes.io/hostname"},
			}},
		}},
		"topologySpreadConstraints": zoneSpread,
	}
	if cell.Sandbox != "" {
		p["runtimeClassName"] = cell.Sandbox
	}
	return p
}
