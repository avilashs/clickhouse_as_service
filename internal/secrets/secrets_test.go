package secrets

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestGetOrCreateIsIdempotentAndRaceFree(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	vals := make([]string, 16)
	for i := range vals {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := s.GetOrCreate(ctx, "clusters/c1/admin", Password(24))
			if err != nil {
				t.Error(err)
			}
			vals[i] = v
		}(i)
	}
	wg.Wait()
	for _, v := range vals {
		if v != vals[0] || len(v) != 24 {
			t.Fatalf("concurrent creates diverged: %q vs %q", v, vals[0])
		}
	}
	// a second store over the same dir (e.g. another worker process) sees the same value
	s2, _ := NewFileStore(dir)
	if v, _ := s2.Get(ctx, "clusters/c1/admin"); v != vals[0] {
		t.Fatal("value not shared across store instances")
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "clusters", "c1", "admin.sec"))
	if strings.Contains(string(raw), vals[0]) {
		t.Fatal("secret stored in plaintext")
	}
	if err := s.DeletePrefix(ctx, "clusters/c1/"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "clusters/c1/admin"); err != ErrNotFound {
		t.Fatalf("after delete: %v", err)
	}
	if _, err := s.Get(ctx, "../etc/passwd"); err == nil {
		t.Fatal("path traversal accepted")
	}
}
