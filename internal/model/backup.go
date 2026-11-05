package model

import "time"

// BackupTarget is where a cell keeps its backups: an S3-compatible bucket. Servers reach it at
// URL (from inside the cell), the control plane at ControlPlaneEndpoint (for retention).
type BackupTarget struct {
	URL                  string `json:"url"` // http(s)://host:port/bucket/, as ClickHouse is given it
	Bucket               string `json:"bucket"`
	Host                 string `json:"host"` // host name or IP the servers connect to
	Port                 int    `json:"port"`
	ControlPlaneEndpoint string `json:"controlPlaneEndpoint"` // host:port for the control plane's own S3 client
	Secure               bool   `json:"secure"`
	// Egress says who the NetworkPolicy lets server pods reach: an in-cluster Service, or CIDRs
	// (cloud: the S3 VPC endpoint).
	EgressNamespace   string            `json:"egressNamespace,omitempty"`
	EgressPodSelector map[string]string `json:"egressPodSelector,omitempty"`
	EgressCIDRs       []string          `json:"egressCidrs,omitempty"`
}

type BackupKind string

const (
	BackupFull        BackupKind = "FULL"
	BackupIncremental BackupKind = "INCREMENTAL"
)

type BackupState string

const (
	BackupRunning   BackupState = "RUNNING"
	BackupCompleted BackupState = "COMPLETED"
	BackupFailed    BackupState = "FAILED"
	BackupDeleting  BackupState = "DELETING"
	BackupDeleted   BackupState = "DELETED"
)

// Backup is one native BACKUP of a cluster's databases.
type Backup struct {
	ID           string      `json:"id"`
	ClusterID    string      `json:"clusterId"`
	ProjectID    string      `json:"projectId"`
	CellID       string      `json:"cellId"`
	Kind         BackupKind  `json:"kind"`
	BaseBackupID string      `json:"baseBackupId,omitempty"`
	State        BackupState `json:"state"`
	Path         string      `json:"-"`
	Scheduled    bool        `json:"scheduled"`
	SizeBytes    int64       `json:"sizeBytes"`
	Error        string      `json:"error,omitempty"`
	Fingerprint  []byte      `json:"-"`
	CreatedAt    time.Time   `json:"createdAt"`
	FinishedAt   *time.Time  `json:"finishedAt,omitempty"`
	VerifiedAt   *time.Time  `json:"verifiedAt,omitempty"`
}

const (
	OpBackupCluster  OperationKind = "BACKUP_CLUSTER"
	OpRestoreCluster OperationKind = "RESTORE_CLUSTER"
	OpVerifyBackup   OperationKind = "VERIFY_BACKUP"
)

// ScratchProject owns the throwaway clusters that restore verification creates; no tenant token
// maps to it, so tenants never see them.
const ScratchProject = "_basalt-verify"

// JobSpec is the work the cell agent does for a backup or restore job. The credentials are
// either short-lived scoped credentials or {{secret:...}} placeholders resolved when the job is
// served; they never reach the cluster's stored spec.
type JobSpec struct {
	Action string `json:"action"` // "backup" | "restore"
	// URL is the object-store prefix of this backup (ends in /); BaseURL the prefix of the backup an
	// incremental one builds on. Restoring an incremental backup needs no BaseURL: the backup's own
	// metadata names its base, and the credentials must be able to read both prefixes.
	URL             string `json:"url"`
	BaseURL         string `json:"baseUrl,omitempty"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	SessionToken    string `json:"sessionToken,omitempty"`
}

// TableFingerprint says what a table held: its rows and an order-independent hash of them.
type TableFingerprint struct {
	Rows uint64 `json:"rows"`
	Hash string `json:"hash"`
}

// JobResult is what the agent reports for a finished backup or restore job.
type JobResult struct {
	// Backup: the fingerprint of every table just before and just after the backup, so that a
	// table written to during the backup is recognised as such.
	Before map[string]TableFingerprint `json:"before,omitempty"`
	After  map[string]TableFingerprint `json:"after,omitempty"`
	// Restore: what the restored cluster holds, and the outcome of CHECK TABLE for each table.
	Restored map[string]TableFingerprint `json:"restored,omitempty"`
	Checks   map[string]string           `json:"checks,omitempty"` // table -> "" if CHECK TABLE passed, else the problem
	Tables   int                         `json:"tables"`
}
