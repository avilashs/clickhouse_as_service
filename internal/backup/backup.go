// Package backup holds what the control plane needs to know about object storage: how to mint
// credentials for one backup or restore, and how to delete a backup's objects. The servers
// themselves talk to the store through ClickHouse's native BACKUP/RESTORE (ADR-0006); the
// control plane never moves backup data.
package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Credentials are what a server is handed for one operation. SessionToken is empty for static
// credentials.
type Credentials struct {
	AccessKey, SecretKey, SessionToken string
	Expires                            time.Time
}

// Scope is what one operation may touch: it may write under WritePrefix (a backup) and read under
// ReadPrefixes (the backup's own chain for an incremental, the source for a restore). Prefixes
// are object-key prefixes inside the bucket, without a leading or trailing slash.
type Scope struct {
	WritePrefix  string
	ReadPrefixes []string
}

// Provider mints credentials for a scope.
type Provider interface {
	Mint(ctx context.Context, s Scope) (Credentials, error)
	// Scoped says whether the credentials are limited to the scope (STS) or are the store's
	// long-lived key (local fixture only).
	Scoped() bool
}

// ScopePolicy is the session policy that limits temporary credentials to a scope: object access
// under the scope's prefixes and nothing else; no listing outside them, no access to other
// clusters' prefixes, no deletes outside the write prefix.
func ScopePolicy(bucket string, s Scope) (string, error) {
	if bucket == "" {
		return "", errors.New("no bucket")
	}
	clean := func(p string) string { return strings.Trim(p, "/") }
	var read, all []string
	for _, p := range s.ReadPrefixes {
		if clean(p) == "" {
			return "", errors.New("empty read prefix would grant the whole bucket")
		}
		read = append(read, "arn:aws:s3:::"+bucket+"/"+clean(p)+"/*")
		all = append(all, clean(p)+"/*")
	}
	type stmt = map[string]any
	var statements []stmt
	if w := clean(s.WritePrefix); w != "" {
		all = append(all, w+"/*")
		statements = append(statements, stmt{
			"Effect":   "Allow",
			"Action":   []string{"s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"},
			"Resource": []string{"arn:aws:s3:::" + bucket + "/" + w + "/*"},
		})
	}
	if len(read) > 0 {
		statements = append(statements, stmt{"Effect": "Allow", "Action": []string{"s3:GetObject"}, "Resource": read})
	}
	if len(all) == 0 {
		return "", errors.New("a scope with nothing to read or write")
	}
	statements = append(statements, stmt{
		"Effect": "Allow", "Action": []string{"s3:ListBucket", "s3:ListBucketMultipartUploads"},
		"Resource":  []string{"arn:aws:s3:::" + bucket},
		"Condition": stmt{"StringLike": stmt{"s3:prefix": all}},
	})
	b, err := json.Marshal(stmt{"Version": "2012-10-17", "Statement": statements})
	return string(b), err
}

// Static returns one long-lived key for every scope. It exists for the local fixture store,
// which has no STS; a cloud cell never uses it.
type Static struct{ AccessKey, SecretKey string }

func (Static) Scoped() bool { return false }
func (s Static) Mint(context.Context, Scope) (Credentials, error) {
	return Credentials{AccessKey: s.AccessKey, SecretKey: s.SecretKey}, nil
}

// STS mints temporary credentials with AssumeRole and a session policy scoped to the operation.
// Against AWS set RoleARN and Region; any S3 store with an AssumeRole-compatible STS works.
type STS struct {
	Endpoint             string // https://sts.amazonaws.com or the store's STS endpoint
	AccessKey, SecretKey string // the control plane's own identity
	RoleARN, Region      string
	Bucket               string
	Duration             time.Duration
}

func (STS) Scoped() bool { return true }

func (p STS) Mint(ctx context.Context, s Scope) (Credentials, error) {
	policy, err := ScopePolicy(p.Bucket, s)
	if err != nil {
		return Credentials{}, err
	}
	d := p.Duration
	if d == 0 {
		d = time.Hour
	}
	c, err := credentials.NewSTSAssumeRole(p.Endpoint, credentials.STSAssumeRoleOptions{
		AccessKey: p.AccessKey, SecretKey: p.SecretKey, Policy: policy, DurationSeconds: int(d.Seconds()),
		Location: p.Region, RoleARN: p.RoleARN, RoleSessionName: "basalt-backup",
	})
	if err != nil {
		return Credentials{}, fmt.Errorf("assume role: %w", err)
	}
	v, err := c.GetWithContext(nil)
	if err != nil {
		return Credentials{}, fmt.Errorf("assume role: %w", err)
	}
	return Credentials{AccessKey: v.AccessKeyID, SecretKey: v.SecretAccessKey, SessionToken: v.SessionToken, Expires: time.Now().Add(d)}, nil
}

// Objects is the control plane's view of the bucket, used for retention.
type Objects interface {
	// DeletePrefix removes every object under prefix and returns how many.
	DeletePrefix(ctx context.Context, prefix string) (int, error)
	// SizeOf returns the total size of the objects under prefix.
	SizeOf(ctx context.Context, prefix string) (int64, error)
}

type S3 struct {
	c      *minio.Client
	bucket string
}

// NewS3 connects with the control plane's own key (it needs list and delete on the bucket).
func NewS3(endpoint string, secure bool, accessKey, secretKey, bucket string) (*S3, error) {
	c, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: secure})
	if err != nil {
		return nil, err
	}
	return &S3{c: c, bucket: bucket}, nil
}

func (o *S3) list(ctx context.Context, prefix string) <-chan minio.ObjectInfo {
	return o.c.ListObjects(ctx, o.bucket, minio.ListObjectsOptions{Prefix: strings.Trim(prefix, "/") + "/", Recursive: true})
}

func (o *S3) SizeOf(ctx context.Context, prefix string) (int64, error) {
	var n int64
	for obj := range o.list(ctx, prefix) {
		if obj.Err != nil {
			return 0, obj.Err
		}
		n += obj.Size
	}
	return n, nil
}

func (o *S3) DeletePrefix(ctx context.Context, prefix string) (int, error) {
	if strings.Trim(prefix, "/") == "" {
		return 0, errors.New("refusing to delete the whole bucket")
	}
	keys := make(chan minio.ObjectInfo)
	go func() {
		defer close(keys)
		for obj := range o.list(ctx, prefix) {
			if obj.Err != nil {
				return
			}
			keys <- obj
		}
	}()
	n := 0
	for e := range o.c.RemoveObjects(ctx, o.bucket, keys, minio.RemoveObjectsOptions{}) {
		if e.Err != nil {
			return n, e.Err
		}
		n++
	}
	// listing errors surface as an unfinished delete: check that nothing is left
	for obj := range o.list(ctx, prefix) {
		if obj.Err != nil {
			return n, obj.Err
		}
		return n, fmt.Errorf("objects remain under %s", prefix)
	}
	return n, nil
}
