package backup

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type policy struct {
	Statement []struct {
		Effect    string
		Action    []string
		Resource  []string
		Condition map[string]map[string][]string
	}
}

func parse(t *testing.T, s string) policy {
	t.Helper()
	var p policy
	if err := json.Unmarshal([]byte(s), &p); err != nil {
		t.Fatalf("%v: %s", err, s)
	}
	return p
}

func TestScopePolicyLimitsAccessToTheClustersPrefix(t *testing.T) {
	js, err := ScopePolicy("bkt", Scope{WritePrefix: "proj/c1/b2", ReadPrefixes: []string{"proj/c1/b1"}})
	if err != nil {
		t.Fatal(err)
	}
	p := parse(t, js)
	var writes, reads, lists bool
	for _, s := range p.Statement {
		for _, r := range s.Resource {
			if strings.Contains(r, "proj/c1/b2/*") && contains(s.Action, "s3:PutObject") {
				writes = true
			}
			if r == "arn:aws:s3:::bkt/proj/c1/b1/*" && contains(s.Action, "s3:GetObject") && !contains(s.Action, "s3:PutObject") && !contains(s.Action, "s3:DeleteObject") {
				reads = true
			}
			if r == "arn:aws:s3:::bkt" && contains(s.Action, "s3:ListBucket") {
				lists = true
				pre := s.Condition["StringLike"]["s3:prefix"]
				if !contains(pre, "proj/c1/b2/*") || !contains(pre, "proj/c1/b1/*") || len(pre) != 2 {
					t.Errorf("listing allowed under %v", pre)
				}
			}
			if r == "arn:aws:s3:::bkt/*" || r == "*" || r == "arn:aws:s3:::*" {
				t.Errorf("a statement covers the whole bucket: %v", s)
			}
		}
	}
	if !writes || !reads || !lists {
		t.Errorf("writes %v reads %v lists %v in %s", writes, reads, lists, js)
	}
	if strings.Contains(js, "other-cluster") {
		t.Error("another cluster's prefix appeared")
	}
}

func TestScopePolicyRefusesScopesThatCouldCoverTheBucket(t *testing.T) {
	if _, err := ScopePolicy("bkt", Scope{ReadPrefixes: []string{"/"}}); err == nil {
		t.Error("an empty read prefix must be refused")
	}
	if _, err := ScopePolicy("bkt", Scope{}); err == nil {
		t.Error("a scope with nothing in it must be refused")
	}
	if _, err := ScopePolicy("", Scope{WritePrefix: "a"}); err == nil {
		t.Error("no bucket must be refused")
	}
}

func TestReadOnlyScopeCannotWrite(t *testing.T) {
	js, _ := ScopePolicy("bkt", Scope{ReadPrefixes: []string{"p/c/b"}})
	for _, s := range parse(t, js).Statement {
		for _, a := range s.Action {
			if strings.HasPrefix(a, "s3:Put") || a == "s3:DeleteObject" {
				t.Errorf("a restore scope can %s", a)
			}
		}
	}
}

func TestStaticProviderIsNotScoped(t *testing.T) {
	var p Provider = Static{AccessKey: "a", SecretKey: "s"}
	if p.Scoped() {
		t.Error("static credentials are not scoped")
	}
	c, _ := p.Mint(context.Background(), Scope{WritePrefix: "x"})
	if c.AccessKey != "a" || c.SessionToken != "" {
		t.Errorf("%+v", c)
	}
	if !(STS{}).Scoped() {
		t.Error("STS credentials are scoped")
	}
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}
