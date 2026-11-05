// Package api is the control plane's REST API (api/openapi.yaml) plus the cell-agent API.
// Mutations are asynchronous: they persist an Operation in the same transaction as the
// desired-state change, then start its workflow. Every mutation needs an Idempotency-Key.
package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/engine/clickhouse"
	"github.com/avilashs/clickhouse_as_service/internal/model"
	"github.com/avilashs/clickhouse_as_service/internal/secrets"
	"github.com/avilashs/clickhouse_as_service/internal/store"
	"github.com/avilashs/clickhouse_as_service/internal/workflows"
)

// DefaultServerVersion is the current LTS channel default (design §13.2).
const DefaultServerVersion = "26.8.34.13"

// Versions are the offered images by channel. A version is offered once the M3 attack suite has
// passed on a cluster running it (design §13.2); 26.8.36.3's result is in docs/m5-report.md.
var Versions = map[string][]string{"lts": {"26.8.34.13", "26.8.36.3"}}

type Server struct {
	Store    *store.Store
	Secrets  secrets.Store
	Temporal client.Client
	Engines  map[string]engine.Engine
	// Tokens maps tenant bearer tokens to project IDs (M1; OIDC later).
	Tokens  map[string]string
	OpenAPI []byte
	Log     *slog.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.register(func(pattern string, h http.HandlerFunc) { mux.HandleFunc(pattern, h) })
	return logRequests(s.Log, mux)
}

// Routes lists every registered "METHOD /path" pattern (the OpenAPI contract test uses it).
func (s *Server) Routes() []string {
	var out []string
	s.register(func(pattern string, _ http.HandlerFunc) { out = append(out, pattern) })
	return out
}

func (s *Server) register(handle func(pattern string, h http.HandlerFunc)) {
	handle("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	handle("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.Write(s.OpenAPI)
	})
	handle("GET /v1/sizes", s.listSizes)
	handle("GET /v1/versions", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]any{"channels": Versions}) })
	handle("POST /v1/projects/{project}/clusters", s.tenant(s.createCluster))
	handle("GET /v1/projects/{project}/clusters", s.tenant(s.listClusters))
	handle("GET /v1/projects/{project}/clusters/{cluster}", s.tenant(s.getCluster))
	handle("PATCH /v1/projects/{project}/clusters/{cluster}", s.tenant(s.updateCluster))
	handle("DELETE /v1/projects/{project}/clusters/{cluster}", s.tenant(s.deleteCluster))
	handle("GET /v1/projects/{project}/clusters/{cluster}/initialCredentials", s.tenant(s.initialCredentials))
	handle("GET /v1/projects/{project}/operations/{operation}", s.tenant(s.getOperation))
	handle("POST /v1/projects/{project}/clusters/{cluster}/backups", s.tenant(s.createBackup))
	handle("GET /v1/projects/{project}/clusters/{cluster}/backups", s.tenant(s.listBackups))
	handle("GET /v1/projects/{project}/clusters/{cluster}/backups/{backup}", s.tenant(s.getBackup))
	handle("DELETE /v1/projects/{project}/clusters/{cluster}/backups/{backup}", s.tenant(s.deleteBackup))
	handle("POST /v1/projects/{project}/clusters/{cluster}/backups/{backup}/restore", s.tenant(s.restoreBackup))
	handle("POST /v1/projects/{project}/clusters/{cluster}/backups/{backup}/verify", s.tenant(s.verifyBackup))
	handle("GET /v1/projects/{project}/clusters/{cluster}/backups/{backup}/verifications", s.tenant(s.listVerifications))
	// cell agent API (outbound from the cell; bearer token per cell, mTLS in the cloud)
	handle("GET /v1/cells/{cell}/desired", s.agent(s.getDesired))
	handle("POST /v1/cells/{cell}/observed", s.agent(s.postObserved))
}

// ---------------------------------------------------------------- errors and helpers

type apiError struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Fields  []engine.FieldError `json:"fields,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, apiError{Code: code, Message: msg})
}

func (s *Server) storeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, 404, "NOT_FOUND", "not found")
	case errors.Is(err, store.ErrConflict):
		writeErr(w, 409, "CONFLICT", err.Error())
	case errors.Is(err, store.ErrIdempotencyMismatch):
		writeErr(w, 422, "IDEMPOTENCY_KEY_REUSED", err.Error())
	case errors.Is(err, store.ErrCapacity):
		writeErr(w, 503, "NO_CAPACITY", "no cell has capacity for this cluster; try again later")
	default:
		s.Log.Error("internal error", "err", err)
		writeErr(w, 500, "INTERNAL", "internal error")
	}
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(h, "Bearer ")
}

// tenant authenticates the caller and scopes the request to the token's project. A project
// the token does not own is reported as 404, so project IDs cannot be probed.
func (s *Server) tenant(h func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := bearer(r)
		project, ok := "", false
		for t, p := range s.Tokens {
			if subtle.ConstantTimeCompare([]byte(t), []byte(tok)) == 1 {
				project, ok = p, true
			}
		}
		if !ok {
			writeErr(w, 401, "UNAUTHENTICATED", "missing or invalid bearer token")
			return
		}
		if r.PathValue("project") != project {
			writeErr(w, 404, "NOT_FOUND", "not found")
			return
		}
		h(w, r, project)
	}
}

func (s *Server) agent(h func(http.ResponseWriter, *http.Request, model.Cell)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cell, tokenHash, err := s.Store.GetCell(r.Context(), r.PathValue("cell"))
		sum := sha256.Sum256([]byte(bearer(r)))
		if err != nil || subtle.ConstantTimeCompare([]byte(hex.EncodeToString(sum[:])), []byte(tokenHash)) != 1 {
			writeErr(w, 401, "UNAUTHENTICATED", "invalid cell credentials")
			return
		}
		h(w, r, cell)
	}
}

func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	k := r.Header.Get("Idempotency-Key")
	if k == "" || len(k) > 200 {
		writeErr(w, 400, "IDEMPOTENCY_KEY_REQUIRED", "mutations require an Idempotency-Key header (1-200 chars)")
		return "", false
	}
	return k, true
}

func requestHash(method, path string, body any) string {
	b, _ := json.Marshal(body)
	sum := sha256.Sum256([]byte(method + " " + path + "\n" + string(b)))
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------- cluster views

type Endpoint struct {
	Host          string `json:"host"`
	NativeTLSPort int    `json:"nativeTlsPort"`
	HTTPSPort     int    `json:"httpsPort"`
	CACertificate string `json:"caCertificate,omitempty"`
}

type ClusterView struct {
	model.Cluster
	Endpoint Endpoint `json:"endpoint"`
}

func (s *Server) view(ctx context.Context, c model.Cluster) ClusterView {
	cell, _, _ := s.Store.GetCell(ctx, c.CellID)
	return ClusterView{Cluster: c, Endpoint: Endpoint{
		Host: model.Endpoint(c.ID, c.CellID, cell.Domain), NativeTLSPort: 9440, HTTPSPort: 8443, CACertificate: cell.CAPEM,
	}}
}

// ---------------------------------------------------------------- handlers

type CreateClusterRequest struct {
	Name       string `json:"name"`
	Size       string `json:"size"`
	Tier       string `json:"tier,omitempty"`
	StorageGiB int    `json:"storageGiB,omitempty"`
	Version    string `json:"version,omitempty"`
	// dedicated tier only: shards and replicas are chosen by the tenant
	Shards           int `json:"shards,omitempty"`
	ReplicasPerShard int `json:"replicasPerShard,omitempty"`
}

var nameRE = regexp.MustCompile(`^[a-z][a-z0-9-]{1,38}[a-z0-9]$`)

func (s *Server) createCluster(w http.ResponseWriter, r *http.Request, project string) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	var req CreateClusterRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeErr(w, 400, "BAD_REQUEST", "invalid JSON body: "+err.Error())
		return
	}
	if !nameRE.MatchString(req.Name) {
		writeJSON(w, 400, apiError{Code: "INVALID", Message: "invalid request", Fields: []engine.FieldError{{Field: "name", Message: "3-40 chars: lowercase letters, digits, hyphens; must start with a letter"}}})
		return
	}
	if req.Tier == "" {
		req.Tier = string(model.TierShared)
		if size, ok := model.Sizes[req.Size]; ok {
			req.Tier = string(size.Tier)
		}
	}
	if req.Version == "" {
		req.Version = DefaultServerVersion
	}
	if size, ok := model.Sizes[req.Size]; ok {
		if req.StorageGiB == 0 {
			req.StorageGiB = size.DefaultStorage
		}
		if req.Shards == 0 {
			req.Shards = 1
		}
		if req.ReplicasPerShard == 0 {
			req.ReplicasPerShard = size.Replicas
		}
	}
	hash := requestHash(r.Method, r.URL.Path, req)

	cell, err := s.Store.PickCell(r.Context())
	if err != nil {
		s.storeErr(w, err)
		return
	}
	spec := model.ClusterSpec{
		Version: 1, Engine: "clickhouse", Tier: model.Tier(req.Tier), Size: req.Size, Shards: req.Shards,
		ReplicasPerShard: req.ReplicasPerShard, StorageGiB: req.StorageGiB, ServerVersion: req.Version, KeeperVersion: req.Version,
	}
	if !versionOffered(req.Version) {
		writeJSON(w, 400, apiError{Code: "INVALID", Message: "invalid request", Fields: []engine.FieldError{{Field: "version", Message: fmt.Sprintf("not offered; see GET /v1/versions")}}})
		return
	}
	if errs := s.Engines["clickhouse"].Validate(spec, cell); len(errs) > 0 {
		writeJSON(w, 400, apiError{Code: "INVALID", Message: "invalid request", Fields: errs})
		return
	}
	now := time.Now().UTC()
	c := model.Cluster{ID: model.NewID("c"), ProjectID: project, CellID: cell.ID, Name: req.Name, State: model.ClusterCreating, Spec: spec, CreatedAt: now}
	op := model.Operation{ID: model.NewID("o"), ProjectID: project, ClusterID: c.ID, Kind: model.OpCreateCluster, State: model.OpPending, CreatedAt: now}
	op, created, err := s.Store.CreateCluster(r.Context(), store.CreateClusterRequest{Cluster: c, Operation: op, IdempotencyKey: key, RequestHash: hash})
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if created {
		_ = s.Store.Audit(r.Context(), "project:"+project, "cluster.create", c.ID, op.ID, map[string]any{"name": c.Name, "size": c.Spec.Size})
	}
	s.startAndRespond(w, r, op)
}

func versionOffered(v string) bool {
	for _, vs := range Versions {
		for _, x := range vs {
			if x == v {
				return true
			}
		}
	}
	return false
}

// startAndRespond starts the operation's workflow (idempotent) and returns 202. If the start
// fails, the operation stays PENDING and the worker's sweeper starts it.
func (s *Server) startAndRespond(w http.ResponseWriter, r *http.Request, op model.Operation) {
	if op.State == model.OpPending {
		if err := workflows.Start(r.Context(), s.Temporal, op); err != nil {
			s.Log.Warn("workflow start failed; sweeper will retry", "operation", op.ID, "err", err)
		}
	}
	if fresh, err := s.Store.GetOperation(r.Context(), op.ID); err == nil {
		op = fresh // report the stored timestamps and any progress the workflow already made
	}
	w.Header().Set("Location", "/v1/projects/"+op.ProjectID+"/operations/"+op.ID)
	writeJSON(w, 202, op)
}

func (s *Server) listClusters(w http.ResponseWriter, r *http.Request, project string) {
	cs, err := s.Store.ListClusters(r.Context(), project)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	out := make([]ClusterView, 0, len(cs))
	for _, c := range cs {
		out = append(out, s.view(r.Context(), c))
	}
	writeJSON(w, 200, map[string]any{"clusters": out})
}

func (s *Server) getCluster(w http.ResponseWriter, r *http.Request, project string) {
	c, err := s.Store.GetCluster(r.Context(), project, r.PathValue("cluster"))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	writeJSON(w, 200, s.view(r.Context(), c))
}

func (s *Server) deleteCluster(w http.ResponseWriter, r *http.Request, project string) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	id := r.PathValue("cluster")
	op := model.Operation{ID: model.NewID("o"), ProjectID: project, ClusterID: id, Kind: model.OpDeleteCluster, State: model.OpPending, CreatedAt: time.Now().UTC()}
	op, created, err := s.Store.StartOperation(r.Context(), op, key, requestHash(r.Method, r.URL.Path, nil))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if created {
		_ = s.Store.Audit(r.Context(), "project:"+project, "cluster.delete", id, op.ID, nil)
	}
	s.startAndRespond(w, r, op)
}

func (s *Server) initialCredentials(w http.ResponseWriter, r *http.Request, project string) {
	c, err := s.Store.GetCluster(r.Context(), project, r.PathValue("cluster"))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	pw, err := s.Secrets.Get(r.Context(), clickhouse.AdminPasswordSecret(c.ID))
	if errors.Is(err, secrets.ErrNotFound) {
		writeErr(w, 409, "NOT_READY", "credentials are created while the cluster is provisioned")
		return
	}
	if err != nil {
		s.storeErr(w, err)
		return
	}
	_ = s.Store.Audit(r.Context(), "project:"+project, "cluster.credentials.read", c.ID, "", nil)
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, map[string]string{"username": clickhouse.CustomerAdmin, "password": pw})
}

func (s *Server) getOperation(w http.ResponseWriter, r *http.Request, project string) {
	op, err := s.Store.GetOperation(r.Context(), r.PathValue("operation"))
	if err != nil || op.ProjectID != project {
		s.storeErr(w, store.ErrNotFound)
		return
	}
	writeJSON(w, 200, op)
}

func (s *Server) listSizes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"sizes": model.Sizes})
}

// ---------------------------------------------------------------- cell agent API

// DesiredResponse is what the agent receives: payloads with secret references resolved.
type DesiredResponse struct {
	Generation int64          `json:"generation"`
	Clusters   []DesiredEntry `json:"clusters"`
	Jobs       []JobEntry     `json:"jobs,omitempty"`
}

// JobEntry is one backup or restore job for the agent to run against a cluster's servers. It is
// served beside the desired state but is not part of it.
type JobEntry struct {
	ID        string        `json:"id"`
	ClusterID string        `json:"clusterId"`
	Spec      model.JobSpec `json:"spec"`
	State     string        `json:"state"` // the control plane's view: PENDING | RUNNING | SUCCEEDED | FAILED
}

// JobReport is what the agent says about a job.
type JobReport struct {
	ID      string          `json:"id"`
	State   string          `json:"state"`
	Message string          `json:"message,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}

type DesiredEntry struct {
	ClusterID  string         `json:"clusterId"`
	Generation int64          `json:"generation"`
	Hash       string         `json:"hash"`
	Tombstone  bool           `json:"tombstone"`
	Desired    engine.Desired `json:"desired"`
}

func (s *Server) getDesired(w http.ResponseWriter, r *http.Request, cell model.Cell) {
	rows, gen, err := s.Store.DesiredForCell(r.Context(), cell.ID)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	resp := DesiredResponse{Generation: gen}
	for _, row := range rows {
		resolved, err := engine.ResolveSecrets(string(row.Payload), func(name string) (string, error) {
			v, err := s.Secrets.Get(r.Context(), name)
			if err == nil && strings.ContainsAny(v, "\"\\") {
				return "", fmt.Errorf("secret %s contains characters unsafe for JSON", name)
			}
			return v, err
		})
		if err != nil {
			s.Log.Error("cannot resolve secrets", "cluster", row.ClusterID, "err", err)
			continue // never serve a payload with unresolved references
		}
		var d engine.Desired
		if err := json.Unmarshal([]byte(resolved), &d); err != nil {
			s.storeErr(w, err)
			return
		}
		resp.Clusters = append(resp.Clusters, DesiredEntry{ClusterID: row.ClusterID, Generation: row.Generation, Hash: row.Hash, Tombstone: row.Tombstone, Desired: d})
	}
	jobs, err := s.Store.JobsForCell(r.Context(), cell.ID)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	for _, j := range jobs {
		if j.State == "SUCCEEDED" || j.State == "FAILED" {
			continue // nothing left to do; the workflow collects the result from the store
		}
		resolved, err := engine.ResolveSecrets(string(j.Spec), func(name string) (string, error) {
			v, err := s.Secrets.Get(r.Context(), name)
			if err == nil && strings.ContainsAny(v, "\"\\") {
				return "", fmt.Errorf("secret %s contains characters unsafe for JSON", name)
			}
			return v, err
		})
		if err != nil {
			s.Log.Error("cannot resolve secrets of a job", "job", j.ID, "err", err)
			continue
		}
		var spec model.JobSpec
		if err := json.Unmarshal([]byte(resolved), &spec); err != nil {
			s.Log.Error("bad job spec", "job", j.ID, "err", err)
			continue
		}
		resp.Jobs = append(resp.Jobs, JobEntry{ID: j.ID, ClusterID: j.ClusterID, Spec: spec, State: j.State})
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, resp)
}

type ObservedRequest struct {
	CAPEM    string            `json:"caPem"`
	Clusters []engine.Observed `json:"clusters"`
	Jobs     []JobReport       `json:"jobs,omitempty"`
}

func (s *Server) postObserved(w http.ResponseWriter, r *http.Request, cell model.Cell) {
	var req ObservedRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&req); err != nil {
		writeErr(w, 400, "BAD_REQUEST", err.Error())
		return
	}
	if err := s.Store.RecordObserved(r.Context(), cell.ID, req.Clusters); err != nil {
		s.storeErr(w, err)
		return
	}
	for _, j := range req.Jobs {
		if j.State != "RUNNING" && j.State != "SUCCEEDED" && j.State != "FAILED" {
			continue
		}
		if err := s.Store.RecordJobStatus(r.Context(), cell.ID, j.ID, j.State, truncateStr(j.Message, 2000), j.Result); err != nil {
			s.storeErr(w, err)
			return
		}
	}
	if err := s.Store.TouchAgent(r.Context(), cell.ID, req.CAPEM); err != nil {
		s.storeErr(w, err)
		return
	}
	w.WriteHeader(204)
}

func logRequests(log *slog.Logger, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: 200}
		h.ServeHTTP(rw, r)
		if !strings.HasPrefix(r.URL.Path, "/v1/cells/") || rw.status >= 400 {
			log.Info("http", "method", r.Method, "path", r.URL.Path, "status", rw.status, "ms", time.Since(start).Milliseconds())
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(c int) { s.status = c; s.ResponseWriter.WriteHeader(c) }

// UpdateClusterRequest changes a live cluster. Only the fields present change.
type UpdateClusterRequest struct {
	Size             *string `json:"size,omitempty"`
	ReplicasPerShard *int    `json:"replicasPerShard,omitempty"`
	StorageGiB       *int    `json:"storageGiB,omitempty"`
	Version          *string `json:"version,omitempty"`
	// FinalizeUpgrade lifts the compatibility pin that an upgrade across minor versions sets.
	FinalizeUpgrade bool `json:"finalizeUpgrade,omitempty"`
}

func (s *Server) updateCluster(w http.ResponseWriter, r *http.Request, project string) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	var req UpdateClusterRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeErr(w, 400, "BAD_REQUEST", "invalid JSON body: "+err.Error())
		return
	}
	id := r.PathValue("cluster")
	c, err := s.Store.GetCluster(r.Context(), project, id)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	cell, _, err := s.Store.GetCell(r.Context(), c.CellID)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	cur, next := c.Spec, c.Spec
	// a spec written before M5 has no Keeper pin and follows the server version: pin it to what
	// is running now, so that a server upgrade never moves Keeper
	next.KeeperVersion = cur.KeeperImageVersion()
	invalid := func(field, msg string) {
		writeJSON(w, 400, apiError{Code: "INVALID", Message: "invalid request", Fields: []engine.FieldError{{Field: field, Message: msg}}})
	}
	if req.Size != nil {
		size, ok := model.Sizes[*req.Size]
		if !ok {
			invalid("size", fmt.Sprintf("unknown size %q; see GET /v1/sizes", *req.Size))
			return
		}
		next.Size = size.Name
		if cur.Tier == model.TierShared {
			next.ReplicasPerShard = size.Replicas // a shared size fixes the replica count
		}
	}
	if req.ReplicasPerShard != nil {
		if cur.Tier == model.TierShared && *req.ReplicasPerShard != model.Sizes[next.Size].Replicas {
			invalid("replicasPerShard", fmt.Sprintf("a shared-tier size fixes the replica count (%s has %d); choose a size with the replicas you want", next.Size, model.Sizes[next.Size].Replicas))
			return
		}
		next.ReplicasPerShard = *req.ReplicasPerShard
	}
	if req.StorageGiB != nil {
		next.StorageGiB = *req.StorageGiB
	}
	if req.Version != nil {
		if !versionOffered(*req.Version) {
			invalid("version", "not offered; see GET /v1/versions")
			return
		}
		next.ServerVersion = *req.Version
	}
	next = clickhouse.PlanUpgrade(cur, next, req.FinalizeUpgrade)
	if next == cur || (next.KeeperVersion != cur.KeeperVersion && func() bool { n := next; n.KeeperVersion = cur.KeeperVersion; return n == cur }()) {
		invalid("", "the request changes nothing")
		return
	}
	if errs := s.Engines[c.Spec.Engine].ValidateUpdate(cur, next, cell); len(errs) > 0 {
		writeJSON(w, 400, apiError{Code: "INVALID", Message: "invalid request", Fields: errs})
		return
	}
	op := model.Operation{ID: model.NewID("o"), ProjectID: project, ClusterID: id, Kind: model.OpUpdateCluster, State: model.OpPending, CreatedAt: time.Now().UTC()}
	op, created, err := s.Store.StartUpdate(r.Context(), project, cur.Version, next, op, key, requestHash(r.Method, r.URL.Path, req))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if created {
		_ = s.Store.Audit(r.Context(), "project:"+project, "cluster.update", id, op.ID, map[string]any{"size": next.Size, "version": next.ServerVersion, "replicas": next.ReplicasPerShard})
	}
	s.startAndRespond(w, r, op)
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
