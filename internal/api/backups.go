package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
	"github.com/avilashs/clickhouse_as_service/internal/store"
	"github.com/avilashs/clickhouse_as_service/internal/workflows"
)

// CreateBackupRequest asks for a backup of a cluster's databases (tables, views and functions;
// not users or grants, which Basalt owns). Kind INCREMENTAL builds on BaseBackupID, or on the
// newest completed backup if that is empty.
type CreateBackupRequest struct {
	Kind         model.BackupKind `json:"kind,omitempty"`
	BaseBackupID string           `json:"baseBackupId,omitempty"`
}

// BackupView is a backup as the API shows it.
type BackupView struct {
	model.Backup
	Verified bool `json:"verified"`
}

func backupView(b model.Backup) BackupView {
	return BackupView{Backup: b, Verified: b.VerifiedAt != nil}
}

func (s *Server) createBackup(w http.ResponseWriter, r *http.Request, project string) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	var req CreateBackupRequest
	if r.ContentLength != 0 {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeErr(w, 400, "BAD_REQUEST", "invalid JSON body: "+err.Error())
			return
		}
	}
	if req.Kind == "" {
		req.Kind = model.BackupFull
	}
	if req.Kind != model.BackupFull && req.Kind != model.BackupIncremental {
		writeJSON(w, 400, apiError{Code: "INVALID", Message: "invalid request", Fields: []engine.FieldError{{Field: "kind", Message: "FULL or INCREMENTAL"}}})
		return
	}
	if req.Kind == model.BackupFull && req.BaseBackupID != "" {
		writeJSON(w, 400, apiError{Code: "INVALID", Message: "invalid request", Fields: []engine.FieldError{{Field: "baseBackupId", Message: "only an INCREMENTAL backup has a base"}}})
		return
	}
	id := r.PathValue("cluster")
	op, b, created, err := workflows.StartBackupOperation(r.Context(), s.Store, s.Temporal, project, id, req.Kind, req.BaseBackupID, false, key, requestHash(r.Method, r.URL.Path, req))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if created {
		_ = s.Store.Audit(r.Context(), "project:"+project, "backup.create", id, op.ID, map[string]any{"backup": b.ID, "kind": b.Kind})
	}
	s.startAndRespond(w, r, op)
}

func (s *Server) listBackups(w http.ResponseWriter, r *http.Request, project string) {
	id := r.PathValue("cluster")
	if _, err := s.Store.GetCluster(r.Context(), project, id); err != nil {
		s.storeErr(w, err)
		return
	}
	bs, err := s.Store.ListBackups(r.Context(), project, id)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	out := make([]BackupView, 0, len(bs))
	for _, b := range bs {
		out = append(out, backupView(b))
	}
	writeJSON(w, 200, map[string]any{"backups": out})
}

func (s *Server) getBackup(w http.ResponseWriter, r *http.Request, project string) {
	b, err := s.Store.GetBackup(r.Context(), project, r.PathValue("backup"))
	if err != nil || b.ClusterID != r.PathValue("cluster") {
		s.storeErr(w, store.ErrNotFound)
		return
	}
	writeJSON(w, 200, backupView(b))
}

// deleteBackup marks a backup for deletion; the objects are removed by the retention loop. A
// backup that later ones are built on cannot be deleted before them.
func (s *Server) deleteBackup(w http.ResponseWriter, r *http.Request, project string) {
	if _, ok := idempotencyKey(w, r); !ok {
		return
	}
	cur, err := s.Store.GetBackup(r.Context(), project, r.PathValue("backup"))
	if err != nil || cur.ClusterID != r.PathValue("cluster") {
		s.storeErr(w, store.ErrNotFound)
		return
	}
	b, err := s.Store.BeginDeleteBackup(r.Context(), project, cur.ID)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	_ = s.Store.Audit(r.Context(), "project:"+project, "backup.delete", b.ClusterID, b.ID, nil)
	writeJSON(w, 202, backupView(b))
}

// RestoreRequest names the new cluster a backup is restored into. A restore never writes to an
// existing cluster. Size defaults to the source's size.
type RestoreRequest struct {
	Name string  `json:"name"`
	Size *string `json:"size,omitempty"`
}

func (s *Server) restoreBackup(w http.ResponseWriter, r *http.Request, project string) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	var req RestoreRequest
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
	b, err := s.Store.GetBackup(r.Context(), project, r.PathValue("backup"))
	if err != nil || b.ClusterID != r.PathValue("cluster") {
		s.storeErr(w, store.ErrNotFound)
		return
	}
	if b.State != model.BackupCompleted {
		writeErr(w, 409, "CONFLICT", fmt.Sprintf("the backup is %s; only a COMPLETED backup can be restored", b.State))
		return
	}
	spec, err := s.Store.LastSpec(r.Context(), b.ClusterID)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	spec.Version, spec.CompatibilityPin = 1, ""
	if req.Size != nil {
		size, ok := model.Sizes[*req.Size]
		if !ok || size.Tier != spec.Tier {
			writeJSON(w, 400, apiError{Code: "INVALID", Message: "invalid request", Fields: []engine.FieldError{{Field: "size", Message: "unknown size, or not in the source cluster's tier; see GET /v1/sizes"}}})
			return
		}
		spec.Size = size.Name
		if spec.Tier == model.TierShared {
			spec.ReplicasPerShard = size.Replicas
		}
	}
	cell, _, err := s.Store.GetCell(r.Context(), b.CellID)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if errs := s.Engines[spec.Engine].Validate(spec, cell); len(errs) > 0 {
		writeJSON(w, 400, apiError{Code: "INVALID", Message: "invalid request", Fields: errs})
		return
	}
	now := time.Now().UTC()
	c := model.Cluster{ID: model.NewID("c"), ProjectID: project, CellID: cell.ID, Name: req.Name, State: model.ClusterCreating, Spec: spec, CreatedAt: now}
	op := model.Operation{ID: model.NewID("o"), ProjectID: project, ClusterID: c.ID, Kind: model.OpRestoreCluster, State: model.OpPending, CreatedAt: now}
	op, created, err := s.Store.CreateCluster(r.Context(), store.CreateClusterRequest{Cluster: c, Operation: op, IdempotencyKey: key,
		RequestHash: requestHash(r.Method, r.URL.Path, req), Params: map[string]string{"backupId": b.ID}})
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if created {
		_ = s.Store.Audit(r.Context(), "project:"+project, "backup.restore", c.ID, op.ID, map[string]any{"backup": b.ID, "name": c.Name})
	}
	s.startAndRespond(w, r, op)
}

// verifyBackup restores the backup into a scratch cluster no tenant can see, checks every table
// and compares the data with what the source held when the backup was taken. It is what the
// scheduler does for every cluster once a day; this runs it on demand. The backup shows
// "verified" only when a verification passed.
func (s *Server) verifyBackup(w http.ResponseWriter, r *http.Request, project string) {
	if _, ok := idempotencyKey(w, r); !ok {
		return
	}
	b, err := s.Store.GetBackup(r.Context(), project, r.PathValue("backup"))
	if err != nil || b.ClusterID != r.PathValue("cluster") {
		s.storeErr(w, store.ErrNotFound)
		return
	}
	if b.State != model.BackupCompleted {
		writeErr(w, 409, "CONFLICT", fmt.Sprintf("the backup is %s; only a COMPLETED backup can be verified", b.State))
		return
	}
	if n, err := s.Store.Count(r.Context(), "restore_verifications", "state='RUNNING'"); err == nil && n > 0 {
		writeErr(w, 409, "CONFLICT", "another restore verification is running; they run one at a time")
		return
	}
	vid, err := workflows.StartVerificationWorkflow(r.Context(), s.Temporal, b.ID, false)
	if err != nil {
		s.Log.Error("cannot start verification", "backup", b.ID, "err", err)
		writeErr(w, 500, "INTERNAL", "cannot start the verification")
		return
	}
	_ = s.Store.Audit(r.Context(), "project:"+project, "backup.verify", b.ClusterID, vid, map[string]any{"backup": b.ID})
	writeJSON(w, 202, map[string]any{"verificationId": vid, "backupId": b.ID})
}

// VerificationView is one restore verification of a backup.
type VerificationView struct {
	ID         string          `json:"id"`
	State      string          `json:"state"` // RUNNING | PASSED | FAILED
	Scheduled  bool            `json:"scheduled"`
	Detail     json.RawMessage `json:"detail"`
	CreatedAt  time.Time       `json:"createdAt"`
	FinishedAt *time.Time      `json:"finishedAt,omitempty"`
}

func (s *Server) listVerifications(w http.ResponseWriter, r *http.Request, project string) {
	b, err := s.Store.GetBackup(r.Context(), project, r.PathValue("backup"))
	if err != nil || b.ClusterID != r.PathValue("cluster") {
		s.storeErr(w, store.ErrNotFound)
		return
	}
	vs, err := s.Store.Verifications(r.Context(), b.ID)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	out := make([]VerificationView, 0, len(vs))
	for _, v := range vs {
		out = append(out, VerificationView{ID: v.ID, State: v.State, Scheduled: v.Scheduled, Detail: v.Detail, CreatedAt: v.CreatedAt, FinishedAt: v.FinishedAt})
	}
	writeJSON(w, 200, map[string]any{"verifications": out})
}
