package api

import (
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	openapi "github.com/avilashs/clickhouse_as_service/api"
)

// TestRoutesMatchOpenAPI keeps the hand-written handlers and api/openapi.yaml in sync: every
// public route is documented, and every documented operation is served. The cell-agent API
// (/v1/cells/...) and /healthz, /openapi.yaml are internal and excluded.
func TestRoutesMatchOpenAPI(t *testing.T) {
	var spec struct {
		Paths map[string]map[string]any `json:"paths"`
	}
	if err := yaml.Unmarshal(openapi.Spec, &spec); err != nil {
		t.Fatal(err)
	}
	documented := map[string]bool{}
	for path, ops := range spec.Paths {
		for method := range ops {
			if method == "parameters" {
				continue
			}
			documented[strings.ToUpper(method)+" "+path] = true
		}
	}
	served := map[string]bool{}
	for _, r := range (&Server{}).Routes() {
		if strings.HasPrefix(r, "GET /healthz") || strings.HasPrefix(r, "GET /openapi.yaml") || strings.Contains(r, " /v1/cells/") {
			continue
		}
		served[r] = true
	}
	var missing, extra []string
	for r := range served {
		if !documented[r] {
			missing = append(missing, r)
		}
	}
	for r := range documented {
		if !served[r] {
			extra = append(extra, r)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("routes not in openapi.yaml: %v\noperations in openapi.yaml with no route: %v", missing, extra)
	}
	if len(served) < 7 {
		t.Fatalf("expected the public routes to be enumerated, got %v", served)
	}
}
