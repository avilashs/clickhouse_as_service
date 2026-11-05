// Package model holds the domain types shared by the API, workflows, engines and the cell agent.
package model

import (
	"crypto/rand"
	"encoding/base32"
	"strings"
	"time"
)

// ClusterState is the externally visible lifecycle state of a tenant cluster.
type ClusterState string

const (
	ClusterCreating ClusterState = "CREATING"
	ClusterActive   ClusterState = "ACTIVE"
	ClusterUpdating ClusterState = "UPDATING"
	ClusterDegraded ClusterState = "DEGRADED"
	ClusterFailed   ClusterState = "FAILED"
	ClusterDeleting ClusterState = "DELETING"
	ClusterDeleted  ClusterState = "DELETED"
)

// OperationState is the state of an asynchronous mutation.
type OperationState string

const (
	OpPending   OperationState = "PENDING"
	OpRunning   OperationState = "RUNNING"
	OpSucceeded OperationState = "SUCCEEDED"
	OpFailed    OperationState = "FAILED"
)

// OperationKind names what an operation does.
type OperationKind string

const (
	OpCreateCluster OperationKind = "CREATE_CLUSTER"
	OpDeleteCluster OperationKind = "DELETE_CLUSTER"
	OpUpdateCluster OperationKind = "UPDATE_CLUSTER"
)

// Tier is shared or dedicated (design §7).
type Tier string

const (
	TierShared    Tier = "shared"
	TierDedicated Tier = "dedicated"
)

// ClusterSpec is the immutable, versioned desired shape of a cluster (cluster_specs row).
type ClusterSpec struct {
	Version          int    `json:"version"`
	Engine           string `json:"engine"`
	Tier             Tier   `json:"tier"`
	Size             string `json:"size"`
	Shards           int    `json:"shards"`
	ReplicasPerShard int    `json:"replicasPerShard"`
	StorageGiB       int    `json:"storageGiB"`
	ServerVersion    string `json:"serverVersion"`
	// KeeperVersion pins the Keeper image. A server upgrade does not move it: Keeper is upgraded
	// on its own (and not by M5). Empty means "same as ServerVersion", for specs written before M5.
	KeeperVersion string `json:"keeperVersion,omitempty"`
	// CompatibilityPin, when set, is rendered as the `compatibility` setting of the default
	// profile so a new binary behaves like the old minor version until the upgrade is finalised.
	CompatibilityPin string `json:"compatibilityPin,omitempty"`
}

// KeeperImageVersion is the version of the Keeper image for the spec.
func (s ClusterSpec) KeeperImageVersion() string {
	if s.KeeperVersion != "" {
		return s.KeeperVersion
	}
	return s.ServerVersion
}

// Cluster is a tenant cluster as stored in the control plane.
type Cluster struct {
	ID          string       `json:"id"`
	ProjectID   string       `json:"projectId"`
	CellID      string       `json:"cellId"`
	Name        string       `json:"name"`
	State       ClusterState `json:"state"`
	StateReason string       `json:"stateReason,omitempty"`
	Spec        ClusterSpec  `json:"spec"`
	CreatedAt   time.Time    `json:"createdAt"`
	UpdatedAt   time.Time    `json:"updatedAt"`
}

// Operation is an asynchronous mutation returned by every mutating API call.
type Operation struct {
	ID        string         `json:"id"`
	ProjectID string         `json:"projectId"`
	ClusterID string         `json:"clusterId"`
	Kind      OperationKind  `json:"kind"`
	State     OperationState `json:"state"`
	Error     string         `json:"error,omitempty"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

// Size is one entry of the size catalog (design §7).
type Size struct {
	Name             string `json:"name"`
	Tier             Tier   `json:"tier"`
	CPU              string `json:"cpu"`      // Kubernetes quantity; requests = limits
	Memory           string `json:"memory"`   // Kubernetes quantity; requests = limits
	MemoryBytes      int64  `json:"-"`        // same as Memory, for ClickHouse settings
	MilliCPU         int64  `json:"-"`        // same as CPU, for capacity accounting
	Replicas         int    `json:"replicas"` // replicas per shard
	MaxThreads       int    `json:"maxThreads"`
	MaxConcurrent    int    `json:"maxConcurrentQueries"`
	DefaultStorage   int    `json:"defaultStorageGiB"`
	MaxStorage       int    `json:"maxStorageGiB"`
	HA               bool   `json:"ha"`
	LocalOnly        bool   `json:"localOnly"` // only offered by cells with profile "local" (kind)
	KeeperReplicas   int    `json:"keeperReplicas"`
	KeeperCPURequest string `json:"-"`
	KeeperCPULimit   string `json:"-"`
	KeeperMemory     string `json:"-"`
	InstanceType     string `json:"instanceType,omitempty"` // dedicated: node type; the pod gets the node minus system reservation
	MaxShards        int    `json:"maxShards,omitempty"`
	MaxReplicas      int    `json:"maxReplicas,omitempty"`
}

const gib = int64(1) << 30

// Sizes is the size catalog. Production sizes follow design §7.1; "t-2" exists only so the
// whole shared-tier path can run on a laptop-sized kind cell.
var Sizes = map[string]Size{
	"t-2": {Name: "t-2", Tier: TierShared, CPU: "1", MilliCPU: 1000, Memory: "2Gi", MemoryBytes: 2 * gib,
		Replicas: 2, MaxThreads: 2, MaxConcurrent: 20, DefaultStorage: 5, MaxStorage: 20, HA: true, LocalOnly: true,
		KeeperReplicas: 3, KeeperCPURequest: "100m", KeeperCPULimit: "500m", KeeperMemory: "256Mi"},
	"dev-1": {Name: "dev-1", Tier: TierShared, CPU: "1", MilliCPU: 1000, Memory: "4Gi", MemoryBytes: 4 * gib,
		Replicas: 1, MaxThreads: 2, MaxConcurrent: 20, DefaultStorage: 50, MaxStorage: 200, HA: false,
		KeeperReplicas: 1, KeeperCPURequest: "250m", KeeperCPULimit: "500m", KeeperMemory: "512Mi"},
	"s-2": {Name: "s-2", Tier: TierShared, CPU: "2", MilliCPU: 2000, Memory: "8Gi", MemoryBytes: 8 * gib,
		Replicas: 2, MaxThreads: 4, MaxConcurrent: 50, DefaultStorage: 100, MaxStorage: 1000, HA: true,
		KeeperReplicas: 3, KeeperCPURequest: "250m", KeeperCPULimit: "500m", KeeperMemory: "1Gi"},
	"s-4": {Name: "s-4", Tier: TierShared, CPU: "4", MilliCPU: 4000, Memory: "16Gi", MemoryBytes: 16 * gib,
		Replicas: 2, MaxThreads: 6, MaxConcurrent: 100, DefaultStorage: 200, MaxStorage: 2000, HA: true,
		KeeperReplicas: 3, KeeperCPURequest: "250m", KeeperCPULimit: "500m", KeeperMemory: "1Gi"},
	// Dedicated sizes: one server pod per node. dt-1 is the kind stand-in for a node type.
	"dt-1": {Name: "dt-1", Tier: TierDedicated, CPU: "1", MilliCPU: 1000, Memory: "2Gi", MemoryBytes: 2 * gib,
		Replicas: 2, MaxThreads: 2, MaxConcurrent: 50, DefaultStorage: 5, MaxStorage: 20, HA: true, LocalOnly: true,
		KeeperReplicas: 3, KeeperCPURequest: "100m", KeeperCPULimit: "500m", KeeperMemory: "256Mi",
		InstanceType: "kind", MaxShards: 3, MaxReplicas: 3},
	// Local-only sizes for the M5 lifecycle tests. A shared-tier size fixes the replica count, so
	// "t-3r" is the same pod as "t-2" with a third replica (add replica), and "t-4r" is the same
	// replica count with bigger pods (vertical scale).
	"t-3r": {Name: "t-3r", Tier: TierShared, CPU: "1", MilliCPU: 1000, Memory: "2Gi", MemoryBytes: 2 * gib,
		Replicas: 3, MaxThreads: 2, MaxConcurrent: 20, DefaultStorage: 5, MaxStorage: 20, HA: true, LocalOnly: true,
		KeeperReplicas: 3, KeeperCPURequest: "100m", KeeperCPULimit: "500m", KeeperMemory: "256Mi"},
	"t-4r": {Name: "t-4r", Tier: TierShared, CPU: "2", MilliCPU: 2000, Memory: "3Gi", MemoryBytes: 3 * gib,
		Replicas: 3, MaxThreads: 4, MaxConcurrent: 30, DefaultStorage: 5, MaxStorage: 20, HA: true, LocalOnly: true,
		KeeperReplicas: 3, KeeperCPURequest: "100m", KeeperCPULimit: "500m", KeeperMemory: "256Mi"},
	// m7g.4xlarge: 16 vCPU / 64 GiB, minus kubelet, DaemonSets and 5% headroom (design §7.2)
	"d-m7g-4xl": {Name: "d-m7g-4xl", Tier: TierDedicated, CPU: "14", MilliCPU: 14000, Memory: "56Gi", MemoryBytes: 56 * gib,
		Replicas: 2, MaxThreads: 14, MaxConcurrent: 300, DefaultStorage: 500, MaxStorage: 16000, HA: true,
		KeeperReplicas: 3, KeeperCPURequest: "250m", KeeperCPULimit: "500m", KeeperMemory: "1Gi",
		InstanceType: "m7g.4xlarge", MaxShards: 16, MaxReplicas: 5},
	"s-8": {Name: "s-8", Tier: TierShared, CPU: "8", MilliCPU: 8000, Memory: "32Gi", MemoryBytes: 32 * gib,
		Replicas: 2, MaxThreads: 12, MaxConcurrent: 150, DefaultStorage: 500, MaxStorage: 4000, HA: true,
		KeeperReplicas: 3, KeeperCPURequest: "250m", KeeperCPULimit: "500m", KeeperMemory: "1Gi"},
}

// Cell describes one data-plane Kubernetes cluster.
type Cell struct {
	ID      string `json:"id"`
	Region  string `json:"region"`
	State   string `json:"state"`   // ACTIVE | DRAINING | SUSPENDED
	Profile string `json:"profile"` // "local" (kind) or "cloud"
	Domain  string `json:"domain"`
	CAPEM   string `json:"caPem,omitempty"`
	// Sandbox is the RuntimeClass shared-tier servers must use ("" = none; ADR-0003).
	Sandbox string `json:"sandbox,omitempty"`
}

var idEncoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// NewID returns a DNS-label-safe random identifier with the given one-letter prefix.
func NewID(prefix string) string {
	b := make([]byte, 7)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return prefix + strings.ToLower(idEncoding.EncodeToString(b))[:11]
}

// Endpoint returns the client hostname of a cluster (design §14).
func Endpoint(clusterID, cellID, domain string) string {
	return clusterID + "." + cellID + "." + domain
}
