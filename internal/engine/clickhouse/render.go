// Package clickhouse implements engine.Engine for ClickHouse on the Altinity operator
// (ADR-0002). Every manifest shape here was first proven by hand on a kind cell against
// ClickHouse 26.8.34 and operator 0.27.4 (docs/m1-report.md, docs/m2-report.md).
package clickhouse

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

const (
	certSecret       = "ch-certs"
	usersSecret      = "ch-users"
	clusterName      = "default"
	keeperCluster    = "keeper"
	nativeTLSPort    = 9440
	httpsPort        = 8443
	interserverPort  = 9010
	keeperClientPort = 2281
	keeperRaftPort   = 9444
	keeperHTTPPort   = 9182 // keeper readiness endpoint used by the operator's probe
	certDir          = "/etc/basalt/certs"
	ProviderUser     = "provider_ops"
	CustomerAdmin    = "admin"
	// AgentClientCN is the subject of the client certificate the cell agent presents as provider_ops
	AgentClientCN = "basalt-agent"
	clickhouseUID = 101
	// labels and taints of the placement model (design §8)
	LabelPool             = "basalt.io/pool"
	LabelDedicatedCluster = "basalt.io/dedicated-cluster"
	LabelComponent        = "basalt.io/component"
	LabelCluster          = "basalt.io/cluster"
	LabelTier             = "basalt.io/tier"
	ComponentServer       = "clickhouse-server"
	ComponentKeeper       = "keeper"
	shardLabel            = "clickhouse.altinity.com/shard"
	chiLabel              = "clickhouse.altinity.com/chi"
)

// Names derived from a cluster ID. They follow the operator's naming conventions, which the
// certificate SANs and the agent's SQL targets depend on.
type names struct{ id, ns, chi, chk string }

func namesFor(id string) names { return names{id: id, ns: "ch-" + id, chi: id, chk: id} }

func (n names) serverHost(shard, replica int) string {
	return fmt.Sprintf("chi-%s-%s-%d-%d", n.chi, clusterName, shard, replica)
}
func (n names) keeperHost(replica int) string {
	return fmt.Sprintf("chk-%s-%s-0-%d", n.chk, keeperCluster, replica)
}
func (n names) serverService() string { return "clickhouse-" + n.chi }
func (n names) keeperService() string { return "keeper-" + n.chk }

// NodePoolName is the name of a dedicated cluster's node pool.
func NodePoolName(id string) string { return "dedicated-" + id }

// SecretNames for a cluster, as stored in the platform secret store.
func ProviderPasswordSecret(id string) string { return "clusters/" + id + "/provider_ops" }
func DefaultPasswordSecret(id string) string  { return "clusters/" + id + "/default" }
func AdminPasswordSecret(id string) string    { return "clusters/" + id + "/admin" }

// Engine is the ClickHouse engine.
type Engine struct{}

var _ engine.Engine = Engine{}

func (Engine) Kind() string { return "clickhouse" }

// Validate checks a requested spec against the size catalog and the cell.
func (Engine) Validate(spec model.ClusterSpec, cell model.Cell) []engine.FieldError {
	var errs []engine.FieldError
	size, ok := model.Sizes[spec.Size]
	if !ok {
		names := make([]string, 0, len(model.Sizes))
		for k, s := range model.Sizes {
			if !s.LocalOnly || cell.Profile == "local" {
				names = append(names, k)
			}
		}
		sort.Strings(names)
		return append(errs, engine.FieldError{Field: "size", Message: fmt.Sprintf("unknown size %q; available: %v", spec.Size, names)})
	}
	if size.LocalOnly && cell.Profile != "local" {
		errs = append(errs, engine.FieldError{Field: "size", Message: fmt.Sprintf("size %q is only available in local cells", spec.Size)})
	}
	if spec.Tier != size.Tier {
		errs = append(errs, engine.FieldError{Field: "tier", Message: fmt.Sprintf("size %s is a %s-tier size", size.Name, size.Tier)})
	}
	switch spec.Tier {
	case model.TierShared:
		if spec.Shards != 1 {
			errs = append(errs, engine.FieldError{Field: "shards", Message: "shared tier clusters have exactly 1 shard"})
		}
		if spec.ReplicasPerShard != size.Replicas {
			errs = append(errs, engine.FieldError{Field: "replicasPerShard", Message: fmt.Sprintf("size %s has %d replicas", size.Name, size.Replicas)})
		}
		if cell.Sandbox == "" && cell.Profile != "local" {
			errs = append(errs, engine.FieldError{Field: "tier", Message: "this cell has no sandbox runtime; shared tier unavailable"})
		}
	case model.TierDedicated:
		if spec.Shards < 1 || spec.Shards > size.MaxShards {
			errs = append(errs, engine.FieldError{Field: "shards", Message: fmt.Sprintf("must be between 1 and %d for size %s", size.MaxShards, size.Name)})
		}
		if spec.ReplicasPerShard < 1 || spec.ReplicasPerShard > size.MaxReplicas {
			errs = append(errs, engine.FieldError{Field: "replicasPerShard", Message: fmt.Sprintf("must be between 1 and %d for size %s", size.MaxReplicas, size.Name)})
		}
	default:
		errs = append(errs, engine.FieldError{Field: "tier", Message: "must be shared or dedicated"})
	}
	if spec.StorageGiB < 1 || spec.StorageGiB > size.MaxStorage {
		errs = append(errs, engine.FieldError{Field: "storageGiB", Message: fmt.Sprintf("must be between 1 and %d for size %s", size.MaxStorage, size.Name)})
	}
	if spec.ServerVersion == "" {
		errs = append(errs, engine.FieldError{Field: "serverVersion", Message: "required"})
	}
	return errs
}

// PlanPlacement returns the capacity the cluster's server pods need. Shared clusters take
// CPU and memory from the shared pool; dedicated clusters take whole nodes (Pods = nodes).
// Keeper runs on the provider-owned control pool and is accounted separately (ADR-0004).
func (Engine) PlanPlacement(spec model.ClusterSpec) (engine.PlacementPlan, error) {
	size, ok := model.Sizes[spec.Size]
	if !ok {
		return engine.PlacementPlan{}, fmt.Errorf("unknown size %q", spec.Size)
	}
	pods := spec.Shards * spec.ReplicasPerShard
	if spec.Tier == model.TierDedicated {
		return engine.PlacementPlan{Pool: string(model.TierDedicated), Pods: pods}, nil
	}
	return engine.PlacementPlan{
		Pool:        string(spec.Tier),
		MilliCPU:    size.MilliCPU * int64(pods),
		MemoryBytes: size.MemoryBytes * int64(pods),
		Pods:        pods,
	}, nil
}

// Render produces the full desired state for a cluster. It is pure.
func (e Engine) Render(in engine.RenderInput) (engine.Desired, error) {
	c := in.Cluster
	if errs := e.Validate(c.Spec, in.Cell); len(errs) > 0 {
		return engine.Desired{}, fmt.Errorf("invalid spec: %v", errs)
	}
	size := model.Sizes[c.Spec.Size]
	n := namesFor(c.ID)
	external := model.Endpoint(c.ID, in.Cell.ID, in.Cell.Domain)

	var serverHosts []string
	for s := 0; s < c.Spec.Shards; s++ {
		for r := 0; r < c.Spec.ReplicasPerShard; r++ {
			serverHosts = append(serverHosts, n.serverHost(s, r))
		}
	}
	var keeperHosts []string
	for r := 0; r < size.KeeperReplicas; r++ {
		keeperHosts = append(keeperHosts, n.keeperHost(r))
	}

	objs := []engine.Object{namespace(n, c), usersSecretObj(n)}
	objs = append(objs, networkPolicies(n, in.Cell)...)
	objs = append(objs, keeper(n, size, c.Spec.KeeperImageVersion()), server(n, c, size, in.Cell))
	for s := 0; s < c.Spec.Shards; s++ {
		objs = append(objs, podDisruptionBudget(n, s))
	}
	if c.Spec.Tier == model.TierShared {
		objs = append(objs, tlsRoute(n, "native-tls", external, nativeTLSPort), tlsRoute(n, "https", external, httpsPort))
	}

	sqlHosts := make([]string, 0, len(serverHosts))
	for _, h := range serverHosts {
		sqlHosts = append(sqlHosts, h+"."+n.ns+".svc.cluster.local")
	}
	d := engine.Desired{
		ClusterID: c.ID,
		Namespace: n.ns,
		Objects:   objs,
		Certificates: []engine.CertRequest{{
			Namespace: n.ns, SecretName: certSecret,
			DNSNames: certDNSNames(n, external, serverHosts, keeperHosts),
		}},
		SQL: &engine.SQLTarget{
			Hosts: sqlHosts, Port: nativeTLSPort, User: ProviderUser, ClientCertificateCN: AgentClientCN, CASecret: certSecret,
		},
		Bootstrap: bootstrapSQL(c.ID, size, c.Spec.StorageGiB),
		Expect: engine.Expectation{
			Roles: []engine.PodExpect{
				{Role: ComponentServer, Count: len(serverHosts), Image: "clickhouse/clickhouse-server:" + c.Spec.ServerVersion, CPU: size.CPU, Memory: size.Memory},
				{Role: ComponentKeeper, Count: size.KeeperReplicas, Image: "clickhouse/clickhouse-keeper:" + c.Spec.KeeperImageVersion()},
			},
			ServerVersion: c.Spec.ServerVersion,
			Pods:          len(serverHosts) + size.KeeperReplicas,
			ServerHosts:   len(serverHosts),
			KeeperNodes:   size.KeeperReplicas,
		},
	}
	if c.Spec.Tier == model.TierDedicated {
		// R2b: node count = server count (rotations add a temporary surge node, M5)
		d.NodePools = []engine.NodePoolRequest{{
			Name: NodePoolName(c.ID), Nodes: len(serverHosts), Zones: 3, InstanceType: size.InstanceType,
			Labels:   map[string]string{LabelPool: "dedicated", LabelDedicatedCluster: c.ID},
			TaintKey: LabelDedicatedCluster, TaintValue: c.ID,
		}}
		d.Expect.PoolNodes = len(serverHosts)
	}
	return d, nil
}

// certDNSNames covers every name a peer, the operator, the agent or a client dials.
func certDNSNames(n names, external string, serverHosts, keeperHosts []string) []string {
	out := []string{external}
	short := append(append(append([]string{}, serverHosts...), keeperHosts...), n.serverService(), n.keeperService())
	for _, h := range short {
		out = append(out, h, h+"."+n.ns, h+"."+n.ns+".svc", h+"."+n.ns+".svc.cluster.local")
	}
	return out
}

func labels(n names, component string) map[string]any {
	return map[string]any{LabelCluster: n.id, LabelComponent: component}
}

func namespace(n names, c model.Cluster) engine.Object {
	return engine.Object{
		"apiVersion": "v1", "kind": "Namespace",
		"metadata": map[string]any{
			"name": n.ns,
			"labels": map[string]any{
				"basalt.io/managed": "true", LabelCluster: n.id, "basalt.io/project": c.ProjectID,
				LabelTier: string(c.Spec.Tier),
				// Pod Security Admission "restricted": non-root, no privilege escalation,
				// all capabilities dropped, seccomp RuntimeDefault, no host namespaces.
				"pod-security.kubernetes.io/enforce": "restricted",
			},
		},
	}
}

// usersSecretObj holds password hashes for XML-defined users. Values are secret references
// resolved by the control plane when the agent fetches desired state.
func usersSecretObj(n names) engine.Object {
	return engine.Object{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": usersSecret, "namespace": n.ns, "labels": labels(n, "credentials")},
		"type":     "Opaque",
		"stringData": map[string]any{
			"default_sha256": engine.SecretSHA256Ref(DefaultPasswordSecret(n.id)),
		},
	}
}

func podSecurity() map[string]any {
	return map[string]any{
		"runAsUser": clickhouseUID, "runAsGroup": clickhouseUID, "fsGroup": clickhouseUID, "runAsNonRoot": true,
		"seccompProfile": map[string]any{"type": "RuntimeDefault"},
	}
}

func containerSecurity() map[string]any {
	return map[string]any{
		"allowPrivilegeEscalation": false,
		"capabilities":             map[string]any{"drop": []any{"ALL"}},
	}
}

func certVolume() (map[string]any, map[string]any) {
	return map[string]any{"name": "certs", "secret": map[string]any{"secretName": certSecret, "defaultMode": 0o440}},
		map[string]any{"name": "certs", "mountPath": certDir, "readOnly": true}
}

const opensslXML = `<clickhouse>
  <openSSL>
    <server>
      <certificateFile>` + certDir + `/server.crt</certificateFile>
      <privateKeyFile>` + certDir + `/server.key</privateKeyFile>
      <caConfig>` + certDir + `/ca.crt</caConfig>
      <!-- relaxed: request a client certificate and verify it against the cell CA when one is
           presented (the provider user authenticates with one); clients without one are fine -->
      <verificationMode>relaxed</verificationMode>
      <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
    </server>
    <client>
      <caConfig>` + certDir + `/ca.crt</caConfig>
      <loadDefaultCAFile>false</loadDefaultCAFile>
      <verificationMode>strict</verificationMode>
      <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
    </client>
  </openSSL>
</clickhouse>
`

// Customer users live in replicated access storage (in Keeper), so they are the same on every
// replica and survive pod replacement; provider users stay in read-only users_xml.
const accessXML = `<clickhouse>
  <user_directories replace="replace">
    <users_xml><path>users.xml</path></users_xml>
    <replicated><zookeeper_path>/clickhouse/access/</zookeeper_path></replicated>
  </user_directories>
</clickhouse>
`

// defaultUserXML strips the stock image's admin rights from "default" (access_management and
// named_collection_control are 1 in users.xml, which also forbids <grants>) and undoes the
// operator's widening of its networks to every pod of the cluster. Nothing logs in as default;
// see users(). Named zz- so it merges after the operator's chop-generated-users.xml.
const defaultUserXML = `<clickhouse>
  <users>
    <default>
      <access_management remove="1"/>
      <named_collection_control remove="1"/>
      <networks replace="1"><ip>::1</ip><ip>127.0.0.1</ip></networks>
    </default>
  </users>
</clickhouse>
`

func keeper(n names, size model.Size, version string) engine.Object {
	vol, mount := certVolume()
	chkSelector := map[string]any{"matchLabels": map[string]any{"clickhouse-keeper.altinity.com/chk": n.chk}}
	return engine.Object{
		"apiVersion": "clickhouse-keeper.altinity.com/v1", "kind": "ClickHouseKeeperInstallation",
		"metadata": map[string]any{"name": n.chk, "namespace": n.ns, "labels": labels(n, ComponentKeeper)},
		"spec": map[string]any{
			"defaults": map[string]any{"templates": map[string]any{"podTemplate": "keeper", "dataVolumeClaimTemplate": "keeper-data"}},
			"configuration": map[string]any{
				"clusters": []any{map[string]any{
					"name": keeperCluster, "secure": "yes", "insecure": "no",
					"layout": map[string]any{"replicasCount": size.KeeperReplicas},
				}},
				"settings": map[string]any{
					"logger/level": "information",
					"listen_host":  "0.0.0.0",
					"keeper_server/four_letter_word_white_list": "ruok,mntr,srvr,stat,conf,isro",
				},
				"files": map[string]any{"basalt-openssl.xml": opensslXML},
			},
			"templates": map[string]any{
				"podTemplates": []any{map[string]any{
					"name":     "keeper",
					"metadata": map[string]any{"labels": labels(n, ComponentKeeper)},
					"spec": map[string]any{
						// Keeper runs on the provider-owned control pool, one replica per node and zone (ADR-0004).
						"nodeSelector":                 map[string]any{LabelPool: "control"},
						"tolerations":                  []any{toleration(LabelPool, "control")},
						"automountServiceAccountToken": false,
						"securityContext":              podSecurity(),
						"affinity": map[string]any{"podAntiAffinity": map[string]any{
							"requiredDuringSchedulingIgnoredDuringExecution": []any{map[string]any{
								"labelSelector": chkSelector, "topologyKey": "kubernetes.io/hostname",
							}},
						}},
						"topologySpreadConstraints": []any{map[string]any{
							"maxSkew": 1, "topologyKey": "topology.kubernetes.io/zone", "whenUnsatisfiable": "DoNotSchedule",
							"labelSelector": chkSelector,
						}},
						"containers": []any{map[string]any{
							"name":            "clickhouse-keeper",
							"image":           "clickhouse/clickhouse-keeper:" + version,
							"imagePullPolicy": "IfNotPresent",
							"securityContext": containerSecurity(),
							"resources": map[string]any{
								// CPU may burst on the control pool only; memory is never overcommitted.
								"requests": map[string]any{"cpu": size.KeeperCPURequest, "memory": size.KeeperMemory},
								"limits":   map[string]any{"cpu": size.KeeperCPULimit, "memory": size.KeeperMemory},
							},
							"volumeMounts": []any{mount},
						}},
						"volumes": []any{vol},
					},
				}},
				"volumeClaimTemplates": []any{map[string]any{
					"name": "keeper-data",
					"spec": map[string]any{
						"accessModes": []any{"ReadWriteOnce"},
						"resources":   map[string]any{"requests": map[string]any{"storage": "1Gi"}},
					},
				}},
			},
		},
	}
}

func toleration(key, value string) map[string]any {
	return map[string]any{"key": key, "operator": "Equal", "value": value, "effect": "NoSchedule"}
}

func server(n names, c model.Cluster, size model.Size, cell model.Cell) engine.Object {
	vol, mount := certVolume()
	image := "clickhouse/clickhouse-server:" + c.Spec.ServerVersion
	podSpec := map[string]any{
		"automountServiceAccountToken": false,
		"securityContext":              podSecurity(),
		"initContainers":               serverInitContainers(n, image, c.Spec.StorageGiB, cell.Profile == "local"),
		"containers": []any{map[string]any{
			"name":            "clickhouse",
			"image":           image,
			"imagePullPolicy": "IfNotPresent",
			"securityContext": containerSecurity(),
			"resources": map[string]any{
				"requests": map[string]any{"cpu": size.CPU, "memory": size.Memory},
				"limits":   map[string]any{"cpu": size.CPU, "memory": size.Memory},
			},
			// The operator's default liveness probe gives a server about 90 seconds to open its
			// port (60s initial delay, then 10 failures 3s apart) and kills it after that. A server
			// loading many tables, or starting on a loaded node, takes longer, and being killed
			// restarts the whole load (M4: seen on the local cell after a VM restart, 5 to 6 restarts
			// per server). The kubelet holds liveness and readiness checks back until this probe
			// succeeds, so startup gets ten minutes and a hung running server is still caught fast.
			"startupProbe": map[string]any{
				"httpGet":       map[string]any{"path": "/ping", "port": "https", "scheme": "HTTPS"},
				"periodSeconds": startupProbePeriod, "failureThreshold": startupProbeFailures, "timeoutSeconds": 2,
			},
			"volumeMounts": []any{mount},
		}},
		"volumes": []any{vol},
	}
	for k, v := range placement(n, c, cell) {
		podSpec[k] = v
	}

	serviceSpec := map[string]any{"type": "ClusterIP", "ports": []any{
		map[string]any{"name": "https", "port": httpsPort},
		map[string]any{"name": "native-tls", "port": nativeTLSPort},
	}}
	serviceMeta := map[string]any{}
	if c.Spec.Tier == model.TierDedicated {
		// dedicated clusters get their own network load balancer (ADR-0005); kind has no LB
		// controller, so the Service stays <pending> there and tests use a port-forward
		serviceSpec["type"] = "LoadBalancer"
		serviceMeta["annotations"] = map[string]any{
			"service.beta.kubernetes.io/aws-load-balancer-type":            "external",
			"service.beta.kubernetes.io/aws-load-balancer-nlb-target-type": "ip",
			"service.beta.kubernetes.io/aws-load-balancer-scheme":          "internet-facing",
		}
	}

	keeperNodes := []any{map[string]any{"host": n.keeperService(), "port": keeperClientPort, "secure": "yes"}}
	return engine.Object{
		"apiVersion": "clickhouse.altinity.com/v1", "kind": "ClickHouseInstallation",
		"metadata": map[string]any{"name": n.chi, "namespace": n.ns, "labels": labels(n, "clickhouse")},
		"spec": map[string]any{
			"defaults": map[string]any{"templates": map[string]any{
				"podTemplate": "server", "dataVolumeClaimTemplate": "data", "serviceTemplate": "client",
			}},
			// Rolling changes (M5, design §8.5): the operator restarts one host at a time and,
			// before it goes on to the next, waits for the restarted host to pass its startup and
			// readiness probes, to be included in the cluster again, for the replicas to catch up
			// (absolute delay below 10 s), and, before it stops a host, for that host's running
			// queries to finish.
			"reconcile": map[string]any{"host": map[string]any{"wait": map[string]any{
				"exclude": "yes", "include": "yes", "queries": "yes",
				"replicas": map[string]any{"all": "yes", "new": "yes", "delay": reconcileReplicaDelaySeconds},
				"probes":   map[string]any{"startup": "yes", "readiness": "yes"},
			}}},
			// The operator connects over HTTPS and verifies against the cell CA.
			"security": map[string]any{"clickhouse": map[string]any{"tls": map[string]any{
				"verify": "Strict", "rootCASecretRef": map[string]any{"name": certSecret, "key": "ca.crt"},
			}}},
			"configuration": map[string]any{
				"zookeeper": map[string]any{"nodes": keeperNodes},
				"clusters": []any{map[string]any{
					"name": clusterName, "secure": "yes", "insecure": "no",
					// our per-shard PDBs replace the operator's single cluster-wide one: a pod
					// matching two PDBs cannot be evicted at all, which blocks node drains (M3)
					"pdbManaged": "no",
					// interserver secret: a Distributed query reaches the other replicas as the
					// user who issued it, with that user's grants and row policies. Without it the
					// replicas log in to each other as "default" (M3 finding: Distributed failed
					// with AUTHENTICATION_FAILED). The operator generates the value inside the
					// cell; it never reaches the control plane.
					"secret": map[string]any{"auto": "true"},
					"layout": map[string]any{"shardsCount": c.Spec.Shards, "replicasCount": c.Spec.ReplicasPerShard},
				}},
				"users":    users(),
				"profiles": profiles(size, c.Spec.StorageGiB, c.Spec.CompatibilityPin),
				"settings": serverSettings(n, c, size, cell),
				"files":    map[string]any{"basalt-openssl.xml": opensslXML, "basalt-access.xml": accessXML, "users.d/zz-basalt-default-user.xml": defaultUserXML},
			},
			"templates": map[string]any{
				"podTemplates": []any{map[string]any{
					"name": "server",
					"metadata": map[string]any{"labels": map[string]any{
						LabelCluster: n.id, LabelComponent: ComponentServer, LabelTier: string(c.Spec.Tier),
					}},
					"spec": podSpec,
				}},
				"volumeClaimTemplates": []any{map[string]any{
					"name": "data",
					"spec": map[string]any{
						"accessModes": []any{"ReadWriteOnce"},
						"resources":   map[string]any{"requests": map[string]any{"storage": fmt.Sprintf("%dGi", c.Spec.StorageGiB)}},
					},
				}},
				"serviceTemplates": []any{map[string]any{
					"name": "client", "generateName": "clickhouse-{chi}", "metadata": serviceMeta, "spec": serviceSpec,
				}},
			},
		},
	}
}

func users() map[string]any {
	return map[string]any{
		"default/password_sha256_hex": secretKeyRef(usersSecret, "default_sha256"),
		"default/networks/ip":         []any{"127.0.0.1", "::1"},
		"default/profile":             "default",
		// Nothing logs in as "default" (Distributed uses the interserver secret). Its address
		// restriction does not cover in-process logins, e.g. a local ClickHouse dictionary source
		// (M3 finding), so it also holds no privilege worth taking.
		"default/grants/query": []any{"GRANT SELECT ON system.one"},
		// The provider user authenticates only with a client certificate signed by the cell CA
		// (CN basalt-agent), never a password: tenants reach the servers through the in-cell gateway,
		// so an IP allowlist alone would let anyone holding a password in (M3 finding).
		ProviderUser + "/ssl_certificates/common_name": AgentClientCN,
		ProviderUser + "/networks/ip":                  []any{"10.0.0.0/8"},
		ProviderUser + "/profile":                      "provider",
		ProviderUser + "/quota":                        "default",
		ProviderUser + "/access_management":            1,
		ProviderUser + "/named_collection_control":     1,
	}
}

// serverMemoryLimit is 85% of the pod's memory, in bytes. It is always explicit: under
// gVisor ClickHouse cannot read its cgroup and would size itself to the node (M2 spike).
func serverMemoryLimit(size model.Size) int64 { return size.MemoryBytes * 85 / 100 }

// Tenant limits live in the server default profile, so users the tenant creates without any
// role or profile inherit them (review A1, tested).
func profiles(size model.Size, storageGiB int, compatibilityPin string) map[string]any {
	mem := strconv.FormatInt(size.MemoryBytes*8/10, 10) // per-query cap: 80% of the pod's memory
	p := map[string]any{
		"default/max_memory_usage":                                                                     mem,
		"default/max_memory_usage_for_user":                                                            mem,
		"default/allow_introspection_functions":                                                        0,
		"default/max_threads":                                                                          size.MaxThreads,
		"default/max_concurrent_queries_for_user":                                                      size.MaxConcurrent,
		"default/database_replicated_allow_only_replicated_engine":                                     1,
		"default/async_insert":                                                                         1,
		"default/wait_for_async_insert":                                                                1,
		"default/constraints/max_memory_usage/min":                                                     1,
		"default/constraints/max_memory_usage/max":                                                     mem,
		"default/constraints/max_memory_usage_for_user/min":                                            1,
		"default/constraints/max_memory_usage_for_user/max":                                            mem,
		"default/constraints/allow_introspection_functions/const":                                      "",
		"default/constraints/max_threads/max":                                                          size.MaxThreads,
		"default/constraints/max_concurrent_queries_for_user/max":                                      size.MaxConcurrent,
		"default/constraints/database_replicated_allow_only_replicated_engine/const":                   "",
		"default/constraints/wait_for_async_insert/const":                                              "",
		"provider/max_memory_usage":                                                                    0,
		"provider/max_threads":                                                                         size.MaxThreads,
		"provider/constraints/max_memory_usage/changeable_in_readonly":                                 "",
		"provider/constraints/max_concurrent_queries_for_user/changeable_in_readonly":                  "",
		"provider/constraints/database_replicated_allow_only_replicated_engine/changeable_in_readonly": "",
	}
	for k, v := range stabilityProfile(storageGiB) {
		p[k] = v
	}
	if compatibilityPin != "" {
		// a new binary behaves like the previous minor version until the upgrade is finalised
		p["default/compatibility"] = compatibilityPin
	}
	return p
}

func serverSettings(n names, c model.Cluster, size model.Size, cell model.Cell) map[string]any {
	s := map[string]any{
		"listen_host":             "0.0.0.0",
		"http_port":               "_removed_",
		"tcp_port":                "_removed_",
		"interserver_http_port":   "_removed_",
		"mysql_port":              "_removed_",
		"postgresql_port":         "_removed_",
		"https_port":              httpsPort,
		"tcp_port_secure":         nativeTLSPort,
		"interserver_https_port":  interserverPort,
		"max_concurrent_queries":  size.MaxConcurrent,
		"max_server_memory_usage": serverMemoryLimit(size),
		// Defence in depth behind the grant model (M3): URL-type functions, engines and
		// dictionary sources may reach no host at all. This is the only SQL-level control on
		// HTTP, MySQL, PostgreSQL, Redis, MongoDB, Cassandra and remote ClickHouse dictionary
		// sources, which ClickHouse does not check against READ ON <source> grants (attack
		// S17g). A section with an entry allows only that entry; ".invalid" never resolves
		// (RFC 2606). Opt-in egress (design §9.3) will add the customer's hosts here.
		"remote_url_allow_hosts/host": remoteHost(cell),
		// executable() and executable UDFs read scripts from here: a directory that does not
		// exist and that nothing can create (read-only image paths, no volume)
		"user_scripts_path": "/nonexistent/basalt-no-user-scripts/",
		"access_control_improvements/table_engines_require_grant":              true,
		"access_control_improvements/enable_read_write_grants":                 true,
		"access_control_improvements/settings_constraints_replace_previous":    true,
		"access_control_improvements/on_cluster_queries_require_cluster_grant": true,
		"access_control_improvements/select_from_system_db_requires_grant":     true,
	}
	if c.Spec.Tier == model.TierShared && cell.Sandbox != "" {
		// gVisor's systrap platform patches syscall instructions in memory, so ClickHouse's
		// startup self-check of its executable fails (code 246) and the server exits (M2 spike).
		s["skip_binary_checksum_checks"] = 1
	}
	for k, v := range stabilityServerSettings(size, c.Spec.StorageGiB, c.Spec.Tier, cell.Profile == "local") {
		s[k] = v
	}
	return s
}

// reconcileReplicaDelaySeconds is the replication delay below which the operator considers a
// replica caught up (design §8.5: absolute_delay < 10 s).
const reconcileReplicaDelaySeconds = 10

// startupProbe budget: 5s x 120 = 10 minutes for a server to open its HTTPS port.
const (
	startupProbePeriod   = 5
	startupProbeFailures = 120
)

// denyAllHost is the single remote_url_allow_hosts entry: a name that cannot exist.
const denyAllHost = "basalt-no-egress.invalid"

// remoteHost is the one host SQL functions may dial: none, unless the cell has a backup target,
// which the servers' own BACKUP and RESTORE need. The network policy lets pods reach nothing but
// that target, and tenants have no grant on the S3 functions or on the backup credentials.
func remoteHost(cell model.Cell) string {
	if cell.Backup != nil && cell.Backup.Host != "" {
		return cell.Backup.Host
	}
	return denyAllHost
}

// waitForKeeperScript blocks until the Keeper client Service has a ready endpoint. A server with
// replicated access storage exits with code 231 when Keeper is unreachable at startup (M1
// finding, confirmed in the upstream source: two attempts, then the constructor throws), and
// the kubelet then backs off for up to five minutes. Waiting in an init container turns that
// into a pod that starts within seconds of quorum returning. The Keeper Service has endpoints
// only for ready Keeper pods, which the operator probes with /ready.
func waitForKeeperScript(n names) string {
	return fmt.Sprintf(`until (exec 3<>/dev/tcp/%s/%d) 2>/dev/null; do echo "waiting for Keeper quorum"; sleep 2; done`,
		n.keeperService(), keeperClientPort)
}

// balloonScript creates the emergency-release file (design §10 "Disk full"): once, as a real
// allocation, never re-created when the volume is full.
func balloonScript(storageGiB int, local bool) string {
	want := DiskReserveBytes(storageGiB, local)
	return fmt.Sprintf(`f=%s; want=%d
if [ "$(stat -c %%s "$f" 2>/dev/null || echo 0)" -lt "$want" ]; then
  rm -f "$f"; fallocate -l "$want" "$f" 2>/dev/null || head -c "$want" /dev/zero > "$f"
fi`, BalloonPath, want)
}

func serverInitContainers(n names, image string, storageGiB int, local bool) []any {
	res := map[string]any{
		"requests": map[string]any{"cpu": "100m", "memory": "64Mi"},
		"limits":   map[string]any{"cpu": "100m", "memory": "64Mi"},
	}
	mk := func(name, script string, mounts ...any) map[string]any {
		return map[string]any{
			"name": name, "image": image, "imagePullPolicy": "IfNotPresent",
			"command":         []any{"bash", "-c", script},
			"securityContext": containerSecurity(), "resources": res, "volumeMounts": mounts,
		}
	}
	return []any{
		mk("wait-for-keeper", waitForKeeperScript(n)),
		mk("balloon", balloonScript(storageGiB, local), map[string]any{"name": "data", "mountPath": "/var/lib/clickhouse"}),
	}
}

func secretKeyRef(name, key string) map[string]any {
	return map[string]any{"valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": name, "key": key}}}
}

func podDisruptionBudget(n names, shard int) engine.Object {
	return engine.Object{
		"apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
		"metadata": map[string]any{"name": fmt.Sprintf("clickhouse-shard-%d", shard), "namespace": n.ns, "labels": labels(n, "clickhouse")},
		"spec": map[string]any{
			"maxUnavailable": 1,
			"selector":       map[string]any{"matchLabels": map[string]any{chiLabel: n.chi, shardLabel: strconv.Itoa(shard)}},
		},
	}
}

func tlsRoute(n names, listener, host string, port int) engine.Object {
	return engine.Object{
		"apiVersion": "gateway.networking.k8s.io/v1alpha2", "kind": "TLSRoute",
		"metadata": map[string]any{"name": listener, "namespace": n.ns, "labels": labels(n, "route")},
		"spec": map[string]any{
			"parentRefs": []any{map[string]any{"name": "shared", "namespace": "basalt-system", "sectionName": listener}},
			"hostnames":  []any{host},
			"rules":      []any{map[string]any{"backendRefs": []any{map[string]any{"name": n.serverService(), "port": port}}}},
		},
	}
}
