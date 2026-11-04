// Package secrets abstracts the platform secret manager (AWS Secrets Manager in the cloud,
// design A6). FileStore is the local implementation: AES-256-GCM encrypted files, created
// atomically so that concurrent or retried activities converge on one value.
package secrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var ErrNotFound = errors.New("secret not found")

// Store is the secret manager contract used by the control plane.
type Store interface {
	// GetOrCreate returns the secret, creating it with gen() if absent. Idempotent.
	GetOrCreate(ctx context.Context, name string, gen func() (string, error)) (string, error)
	Get(ctx context.Context, name string) (string, error)
	// DeletePrefix removes every secret under prefix (e.g. "clusters/<id>/").
	DeletePrefix(ctx context.Context, prefix string) error
}

var nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9/_\-.]*$`)

type FileStore struct {
	dir  string
	aead cipher.AEAD
}

// NewFileStore opens (or initialises) a store in dir. The 256-bit key lives in dir/.key.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	keyPath := filepath.Join(dir, ".key")
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	if f, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600); err == nil {
		_, werr := f.Write(key)
		if cerr := f.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			return nil, werr
		}
	} else if !errors.Is(err, fs.ErrExist) {
		return nil, err
	}
	key, err := os.ReadFile(keyPath)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("read key %s: %v", keyPath, err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &FileStore{dir: dir, aead: aead}, nil
}

func (s *FileStore) path(name string) (string, error) {
	if !nameRE.MatchString(name) || strings.Contains(name, "..") {
		return "", fmt.Errorf("invalid secret name %q", name)
	}
	return filepath.Join(s.dir, filepath.FromSlash(name)+".sec"), nil
}

func (s *FileStore) seal(name, v string) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return s.aead.Seal(nonce, nonce, []byte(v), []byte(name)), nil
}

func (s *FileStore) open(name string, b []byte) (string, error) {
	ns := s.aead.NonceSize()
	if len(b) < ns {
		return "", fmt.Errorf("secret %s is corrupt", name)
	}
	pt, err := s.aead.Open(nil, b[:ns], b[ns:], []byte(name))
	if err != nil {
		return "", fmt.Errorf("secret %s: %w", name, err)
	}
	return string(pt), nil
}

func (s *FileStore) Get(_ context.Context, name string) (string, error) {
	p, err := s.path(name)
	if err != nil {
		return "", err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return s.open(name, b)
}

func (s *FileStore) GetOrCreate(ctx context.Context, name string, gen func() (string, error)) (string, error) {
	if v, err := s.Get(ctx, name); err == nil {
		return v, nil
	} else if !errors.Is(err, ErrNotFound) {
		return "", err
	}
	p, _ := s.path(name)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return "", err
	}
	v, err := gen()
	if err != nil {
		return "", err
	}
	sealed, err := s.seal(name, v)
	if err != nil {
		return "", err
	}
	// write to a temp file, then link into place: link fails if another writer won the race,
	// in which case we return the winner's value
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(sealed); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Link(tmp.Name(), p); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", err
	}
	return s.Get(ctx, name)
}

func (s *FileStore) DeletePrefix(_ context.Context, prefix string) error {
	p, err := s.path(strings.TrimSuffix(prefix, "/") + "/x")
	if err != nil {
		return err
	}
	return os.RemoveAll(filepath.Dir(p))
}

const alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

// Password returns a generator for random passwords of n characters.
func Password(n int) func() (string, error) {
	return func() (string, error) {
		var b strings.Builder
		max := big.NewInt(int64(len(alphabet)))
		for i := 0; i < n; i++ {
			k, err := rand.Int(rand.Reader, max)
			if err != nil {
				return "", err
			}
			b.WriteByte(alphabet[k.Int64()])
		}
		return b.String(), nil
	}
}
