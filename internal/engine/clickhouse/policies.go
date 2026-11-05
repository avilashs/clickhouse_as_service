package clickhouse

import (
	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

// Pod selectors use the operator's own labels, which it always sets.
func serverPods(n names) map[string]any {
	return map[string]any{"matchLabels": map[string]any{"clickhouse.altinity.com/chi": n.chi}}
}

func keeperPods(n names) map[string]any {
	return map[string]any{"matchLabels": map[string]any{"clickhouse-keeper.altinity.com/chk": n.chk}}
}

func fromNamespace(name string) map[string]any {
	return map[string]any{"namespaceSelector": map[string]any{"matchLabels": map[string]any{"kubernetes.io/metadata.name": name}}}
}

func tcp(ports ...int) []any {
	out := make([]any, 0, len(ports))
	for _, p := range ports {
		out = append(out, map[string]any{"protocol": "TCP", "port": p})
	}
	return out
}

func netpol(n names, name string, spec map[string]any) engine.Object {
	return engine.Object{
		"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
		"metadata": map[string]any{"name": name, "namespace": n.ns, "labels": labels(n, "network")},
		"spec":     spec,
	}
}

// networkPolicies: default-deny ingress and egress for the whole namespace, then exactly the
// flows the cluster needs (design §9.2, §9.5). There is no egress to anything outside the
// namespace except DNS, so SQL functions that dial out (url, s3, remote, ...) cannot reach
// cloud metadata, other tenants, or the internet even if a grant were mistakenly given.
func networkPolicies(n names, cell model.Cell) []engine.Object {
	return []engine.Object{
		netpol(n, "default-deny", map[string]any{
			"podSelector": map[string]any{},
			"policyTypes": []any{"Ingress", "Egress"},
		}),
		netpol(n, "allow-dns", map[string]any{
			"podSelector": map[string]any{},
			"policyTypes": []any{"Egress"},
			"egress": []any{map[string]any{
				"to": []any{map[string]any{
					"namespaceSelector": map[string]any{"matchLabels": map[string]any{"kubernetes.io/metadata.name": "kube-system"}},
					"podSelector":       map[string]any{"matchLabels": map[string]any{"k8s-app": "kube-dns"}},
				}},
				"ports": []any{map[string]any{"protocol": "UDP", "port": 53}, map[string]any{"protocol": "TCP", "port": 53}},
			}},
		}),
		netpol(n, "server", map[string]any{
			"podSelector": serverPods(n),
			"policyTypes": []any{"Ingress", "Egress"},
			"ingress": []any{
				// tenant clients through the shared gateway
				map[string]any{"from": []any{fromNamespace("envoy-gateway-system")}, "ports": tcp(nativeTLSPort, httpsPort)},
				// cell agent (bootstrap SQL, probes) and the operator (reconcile, metrics)
				map[string]any{"from": []any{fromNamespace("basalt-system")}, "ports": tcp(nativeTLSPort, httpsPort)},
				map[string]any{"from": []any{fromNamespace("kube-system")}, "ports": tcp(nativeTLSPort, httpsPort)},
				// replicas of the same cluster: distributed queries and replication fetches
				map[string]any{"from": []any{map[string]any{"podSelector": serverPods(n)}}, "ports": tcp(nativeTLSPort, httpsPort, interserverPort)},
			},
			"egress": append([]any{
				map[string]any{"to": []any{map[string]any{"podSelector": keeperPods(n)}}, "ports": tcp(keeperClientPort)},
				map[string]any{"to": []any{map[string]any{"podSelector": serverPods(n)}}, "ports": tcp(nativeTLSPort, httpsPort, interserverPort)},
			}, backupEgress(cell)...),
		}),
		netpol(n, "keeper", map[string]any{
			"podSelector": keeperPods(n),
			"policyTypes": []any{"Ingress", "Egress"},
			"ingress": []any{
				map[string]any{"from": []any{map[string]any{"podSelector": serverPods(n)}}, "ports": tcp(keeperClientPort)},
				map[string]any{"from": []any{map[string]any{"podSelector": keeperPods(n)}}, "ports": tcp(keeperClientPort, keeperRaftPort)},
				map[string]any{"from": []any{fromNamespace("kube-system")}, "ports": tcp(keeperClientPort, keeperHTTPPort)},
			},
			"egress": []any{
				map[string]any{"to": []any{map[string]any{"podSelector": keeperPods(n)}}, "ports": tcp(keeperClientPort, keeperRaftPort)},
			},
		}),
	}
}

// backupEgress lets server pods reach the cell's backup target and nothing else outside the
// namespace: an in-cluster Service (local cells, the test fixture) or the object store's address
// range (cloud: the S3 gateway endpoint).
func backupEgress(cell model.Cell) []any {
	t := cell.Backup
	if t == nil {
		return nil
	}
	var to []any
	if t.EgressNamespace != "" {
		peer := fromNamespace(t.EgressNamespace)
		if len(t.EgressPodSelector) > 0 {
			sel := map[string]any{}
			for k, v := range t.EgressPodSelector {
				sel[k] = v
			}
			peer["podSelector"] = map[string]any{"matchLabels": sel}
		}
		to = append(to, peer)
	}
	for _, cidr := range t.EgressCIDRs {
		to = append(to, map[string]any{"ipBlock": map[string]any{"cidr": cidr}})
	}
	if len(to) == 0 {
		return nil
	}
	return []any{map[string]any{"to": to, "ports": tcp(t.Port)}}
}
