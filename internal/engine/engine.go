// Package engine defines the contract between the engine-neutral platform core and a database
// engine (ClickHouse now, Kafka later). Render and Observe are pure functions: the same input
// always yields the same output, which is what makes workflow activities idempotent and lets
// renders be golden-tested.
package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/avilashs/clickhouse_as_service/internal/model"
)

// Object is a Kubernetes object in unstructured form.
type Object = map[string]any

// CertRequest asks the cell agent to keep a TLS Secret (ca.crt, server.crt, server.key)
// signed by the cell CA, covering exactly DNSNames.
type CertRequest struct {
	Namespace  string   `json:"namespace"`
	SecretName string   `json:"secretName"`
	DNSNames   []string `json:"dnsNames"`
}

// SQLTarget tells the agent how to reach a cluster from inside the cell as the provider user.
type SQLTarget struct {
	Hosts       []string `json:"hosts"` // in-cluster DNS names, one per replica
	Port        int      `json:"port"`
	User        string   `json:"user"`
	PasswordRef string   `json:"passwordRef"` // secret reference, resolved by the control plane when served
	CASecret    string   `json:"caSecret"`    // Secret in the cluster namespace holding ca.crt
}

// Desired is everything a cell agent needs to converge one cluster.
type Desired struct {
	ClusterID    string        `json:"clusterId"`
	Namespace    string        `json:"namespace"`
	Objects      []Object      `json:"objects"`      // applied in order with server-side apply
	Certificates []CertRequest `json:"certificates"` // ensured before Objects that mount them
	SQL          *SQLTarget    `json:"sql,omitempty"`
	Bootstrap    []string      `json:"bootstrap,omitempty"` // idempotent SQL, run once per payload hash
	Expect       Expectation   `json:"expect"`
}

// Expectation is what Observe checks against.
type Expectation struct {
	Pods        int `json:"pods"`
	ServerHosts int `json:"serverHosts"` // hosts each replica must see in system.clusters
	KeeperNodes int `json:"keeperNodes"`
}

// Hash is a stable digest of the desired payload; unchanged hash = nothing to publish.
func (d Desired) Hash() string {
	b, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// HostProbe is the agent's in-cell SQL check of one replica.
type HostProbe struct {
	Host         string `json:"host"`
	OK           bool   `json:"ok"`
	ClusterHosts int    `json:"clusterHosts"`
	KeeperOK     bool   `json:"keeperOk"`
	Error        string `json:"error,omitempty"`
}

// Observed is what the agent reports for one cluster.
type Observed struct {
	ClusterID         string            `json:"clusterId"`
	AppliedGeneration int64             `json:"appliedGeneration"`
	AppliedHash       string            `json:"appliedHash"`
	NamespaceExists   bool              `json:"namespaceExists"`
	Objects           map[string]string `json:"objects"` // "Kind/name" -> status (e.g. CHI .status.status)
	PodsReady         int               `json:"podsReady"`
	PodsTotal         int               `json:"podsTotal"`
	BootstrapHash     string            `json:"bootstrapHash"` // payload hash whose bootstrap SQL succeeded
	Probes            []HostProbe       `json:"probes"`
	Error             string            `json:"error,omitempty"`
	ObservedAt        time.Time         `json:"observedAt"`
}

// Phase is the engine's verdict on observed state.
type Phase string

const (
	PhaseProgressing Phase = "Progressing"
	PhaseReady       Phase = "Ready"
	PhaseDegraded    Phase = "Degraded"
	PhaseGone        Phase = "Gone"
)

// Health is Observe's result.
type Health struct {
	Phase   Phase    `json:"phase"`
	Reasons []string `json:"reasons,omitempty"`
}

// FieldError is a validation failure on one request field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e FieldError) Error() string { return e.Field + ": " + e.Message }

// PlacementPlan is the capacity a cluster needs, reserved transactionally before provisioning.
type PlacementPlan struct {
	Pool        string `json:"pool"`
	MilliCPU    int64  `json:"milliCpu"`
	MemoryBytes int64  `json:"memoryBytes"`
	Pods        int    `json:"pods"`
}

// RenderInput is everything Render may depend on. Nothing else (no clock, no randomness).
type RenderInput struct {
	Cluster model.Cluster
	Cell    model.Cell
}

// Engine is implemented once per database engine.
type Engine interface {
	Kind() string
	Validate(spec model.ClusterSpec, cell model.Cell) []FieldError
	PlanPlacement(spec model.ClusterSpec) (PlacementPlan, error)
	Render(in RenderInput) (Desired, error)
	Observe(desired Desired, generation int64, obs *Observed) Health
}

// Secret references. Render emits these placeholders instead of secret values; the control
// plane substitutes them only when serving desired state to the authenticated cell agent, so
// secret values are never stored in PostgreSQL (design §4).
var secretRefRE = regexp.MustCompile(`\{\{secret:([a-z0-9/_\-.]+)(\|sha256)?\}\}`)

// SecretRef returns the placeholder for a secret value.
func SecretRef(name string) string { return "{{secret:" + name + "}}" }

// SecretSHA256Ref returns the placeholder for the hex SHA-256 of a secret value.
func SecretSHA256Ref(name string) string { return "{{secret:" + name + "|sha256}}" }

// ResolveSecrets replaces every placeholder in s using lookup.
func ResolveSecrets(s string, lookup func(name string) (string, error)) (string, error) {
	var firstErr error
	out := secretRefRE.ReplaceAllStringFunc(s, func(m string) string {
		parts := secretRefRE.FindStringSubmatch(m)
		v, err := lookup(parts[1])
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("resolve secret %q: %w", parts[1], err)
			}
			return m
		}
		if parts[2] == "|sha256" {
			sum := sha256.Sum256([]byte(v))
			return hex.EncodeToString(sum[:])
		}
		return v
	})
	return out, firstErr
}

// SecretNames lists the secret names referenced in s.
func SecretNames(s string) []string {
	var out []string
	for _, m := range secretRefRE.FindAllStringSubmatch(s, -1) {
		out = append(out, m[1])
	}
	return out
}
