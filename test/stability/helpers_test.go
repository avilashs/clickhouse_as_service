//go:build stability

// Package stability is the M4 suite: it drives live clusters of the local cell and checks the
// stability controls of design §10 and failure modes F-4, F-5, F-7 and F-8. Run it with
// test/stability/run.sh. The helpers mirror test/attack (a separate package, so they are
// repeated here rather than shared).
package stability

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avilashs/clickhouse_as_service/internal/engine/clickhouse"
)

type cluster struct {
	project, token, id string
	host, ca, adminPW  string
}

type env struct {
	api, gatewayHTTPS string
	a, b              cluster
}

var (
	envOnce sync.Once
	theEnv  *env
)

func getenv(t *testing.T, k string) string {
	t.Helper()
	v := os.Getenv(k)
	if v == "" {
		t.Skipf("%s not set (run test/stability/run.sh)", k)
	}
	return v
}

func setup(t *testing.T) *env {
	t.Helper()
	envOnce.Do(func() {
		e := &env{
			api: getenv(t, "STAB_API"), gatewayHTTPS: getenv(t, "STAB_GATEWAY_HTTPS"),
			a: cluster{project: getenv(t, "STAB_PROJECT_A"), token: getenv(t, "STAB_TOKEN_A"), id: getenv(t, "STAB_CLUSTER_A")},
			b: cluster{project: getenv(t, "STAB_PROJECT_B"), token: getenv(t, "STAB_TOKEN_B"), id: getenv(t, "STAB_CLUSTER_B")},
		}
		for _, c := range []*cluster{&e.a, &e.b} {
			var info struct {
				Endpoint struct{ Host, CACertificate string } `json:"endpoint"`
			}
			st, body := e.apiGet("/v1/projects/"+c.project+"/clusters/"+c.id, c.token)
			if st != 200 || json.Unmarshal(body, &info) != nil {
				t.Fatalf("describe %s: %d %s", c.id, st, body)
			}
			c.host, c.ca = info.Endpoint.Host, info.Endpoint.CACertificate
			var cred struct{ Password string }
			st, body = e.apiGet("/v1/projects/"+c.project+"/clusters/"+c.id+"/initialCredentials", c.token)
			if st != 200 || json.Unmarshal(body, &cred) != nil {
				t.Fatalf("credentials %s: %d", c.id, st)
			}
			c.adminPW = cred.Password
		}
		theEnv = e
	})
	if theEnv == nil {
		t.Fatal("setup failed")
	}
	return theEnv
}

func (e *env) apiGet(path, token string) (int, []byte) {
	req, _ := http.NewRequest("GET", e.api+path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := (&http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return 0, []byte(err.Error())
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// ---------------------------------------------------------------- SQL over HTTPS via the gateway

type client struct {
	http     *http.Client
	host     string
	user, pw string
	certAuth bool
}

func (e *env) connect(c cluster, user, pw string, cert *tls.Certificate) *client {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(c.ca))
	tc := &tls.Config{RootCAs: pool, ServerName: c.host, MinVersion: tls.VersionTLS12}
	if cert != nil {
		tc.Certificates = []tls.Certificate{*cert}
	}
	tr := &http.Transport{
		Proxy: nil, TLSClientConfig: tc, MaxIdleConnsPerHost: 64,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", e.gatewayHTTPS)
		},
	}
	return &client{http: &http.Client{Transport: tr, Timeout: 120 * time.Second}, host: c.host, user: user, pw: pw, certAuth: cert != nil}
}

func (e *env) admin(c cluster) *client { return e.connect(c, clickhouse.CustomerAdmin, c.adminPW, nil) }

var codeRE = regexp.MustCompile(`Code: (\d+)`)

type result struct {
	body string
	code int // ClickHouse error code, 0 on success, -1 for an unparsed HTTP error
	err  string
	dur  time.Duration
}

func (r result) String() string {
	switch {
	case r.err != "":
		return "transport error: " + r.err
	case r.code != 0:
		return fmt.Sprintf("code %d: %.220s", r.code, strings.TrimSpace(r.body))
	}
	return fmt.Sprintf("ok: %.120s", strings.TrimSpace(r.body))
}

func (r result) ok() bool { return r.err == "" && r.code == 0 }

func (c *client) q(sql string) result { return c.qp(sql, nil) }

func (c *client) qp(sql string, params url.Values) result {
	v := url.Values{"default_format": {"TSV"}}
	for k, vs := range params {
		v[k] = vs
	}
	req, _ := http.NewRequest("POST", "https://"+c.host+"/?"+v.Encode(), strings.NewReader(sql))
	req.Header.Set("X-ClickHouse-User", c.user)
	if c.certAuth {
		req.Header.Set("X-ClickHouse-SSL-Certificate-Auth", "on")
	} else {
		req.Header.Set("X-ClickHouse-Key", c.pw)
	}
	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return result{err: err.Error(), dur: time.Since(start)}
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	r := result{body: string(b), dur: time.Since(start)}
	if resp.StatusCode != 200 {
		r.code = -1
		if m := codeRE.FindStringSubmatch(r.body); m != nil {
			r.code, _ = strconv.Atoi(m[1])
		}
	}
	return r
}

func must(t *testing.T, r result) string {
	t.Helper()
	if !r.ok() {
		t.Fatalf("expected success: %s", r)
	}
	return strings.TrimSpace(r.body)
}

func intOf(t *testing.T, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		t.Fatalf("not a number: %q", s)
	}
	return n
}

// ---------------------------------------------------------------- kubectl

func kubectl(args ...string) (string, error) {
	out, err := exec.Command("kubectl", args...).CombinedOutput()
	return string(out), err
}

func run(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}

func (c cluster) ns() string { return "ch-" + c.id }
func (c cluster) pod(replica int) string {
	return fmt.Sprintf("chi-%s-default-0-%d-0", c.id, replica)
}

// podSQL runs a statement inside a server pod with its own clickhouse-client, as the tenant's
// admin, so the statement reaches that replica and no other (the gateway picks replicas).
func (c cluster) podSQL(replica int, sql string) (string, error) {
	return c.podSQLAs(replica, "admin", c.adminPW, sql)
}

func (c cluster) podSQLAs(replica int, user, pw, sql string) (string, error) {
	host := fmt.Sprintf("chi-%s-default-0-%d.ch-%s.svc.cluster.local", c.id, replica, c.id)
	script := fmt.Sprintf(`printf '<clickhouse><openSSL><client><caConfig>/etc/basalt/certs/ca.crt</caConfig><verificationMode>strict</verificationMode></client></openSSL></clickhouse>' > /tmp/cc.xml && `+
		`clickhouse-client --config-file /tmp/cc.xml --secure --host %s --port 9440 --user "$3" --password "$1" -q "$2"`, host)
	out, err := kubectl("-n", c.ns(), "exec", c.pod(replica), "-c", "clickhouse", "--", "bash", "-c", script, "x", pw, sql, user)
	return strings.TrimSpace(out), err
}

type podStatus struct {
	UID      string
	Phase    string
	Restarts int
	OOM      bool
	Init     string // state of the init containers: "done" or the name of the one still running
	Ready    bool
}

func (c cluster) status(replica int) podStatus {
	out, err := kubectl("-n", c.ns(), "get", "pod", c.pod(replica), "-o", "json")
	if err != nil {
		return podStatus{Phase: "missing"}
	}
	var p struct {
		Metadata struct{ UID string }
		Status   struct {
			Phase                 string
			InitContainerStatuses []struct {
				Name  string
				State struct{ Terminated *struct{ ExitCode int } }
			}
			ContainerStatuses []struct {
				Name         string
				RestartCount int
				Ready        bool
				LastState    struct{ Terminated *struct{ Reason string } }
			}
		}
	}
	if json.Unmarshal([]byte(out), &p) != nil {
		return podStatus{Phase: "unparsed"}
	}
	s := podStatus{UID: p.Metadata.UID, Phase: p.Status.Phase, Init: "done"}
	for _, ic := range p.Status.InitContainerStatuses {
		if ic.State.Terminated == nil || ic.State.Terminated.ExitCode != 0 {
			s.Init = ic.Name
			break
		}
	}
	if len(p.Status.InitContainerStatuses) == 0 {
		s.Init = "none"
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Name == "clickhouse" {
			s.Restarts, s.Ready = cs.RestartCount, cs.Ready
			s.OOM = cs.LastState.Terminated != nil && cs.LastState.Terminated.Reason == "OOMKilled"
		}
	}
	return s
}

// ---------------------------------------------------------------- the platform's own account

func cellCertificate(t *testing.T, cn string) tls.Certificate {
	t.Helper()
	out, err := kubectl("-n", "basalt-system", "get", "secret", "basalt-cell-ca", "-o", "jsonpath={.data.tls\\.crt} {.data.tls\\.key}")
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Fields(out)
	if len(parts) != 2 {
		t.Fatalf("cell CA secret: %q", out)
	}
	dec := func(s string) []byte {
		cmd := exec.Command("base64", "-d")
		cmd.Stdin = strings.NewReader(s)
		b, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	crt, key := dec(parts[0]), dec(parts[1])
	cb, _ := pem.Decode(crt)
	kb, _ := pem.Decode(key)
	cert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	k, err := x509.ParseECPrivateKey(kb.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	// the same certificate the cell agent mints (internal/agent): clientAuth, CN basalt-agent
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, cert, &leafKey.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	c := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: leafKey}
	return c
}

func (e *env) provider(t *testing.T, c cluster) *client {
	cert := cellCertificate(t, clickhouse.AgentClientCN)
	return e.connect(c, clickhouse.ProviderUser, "", &cert)
}

// ---------------------------------------------------------------- measuring

type latencies []time.Duration

func (l latencies) pct(p float64) time.Duration {
	if len(l) == 0 {
		return 0
	}
	s := append(latencies{}, l...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(float64(len(s)-1) * p)
	return s[i]
}

// probe runs sql every interval until stop is closed and records latencies and failures.
func probe(c *client, sql string, interval time.Duration, stop <-chan struct{}) (latencies, []result) {
	var lat latencies
	var fails []result
	for {
		select {
		case <-stop:
			return lat, fails
		default:
		}
		r := c.q(sql)
		if r.ok() {
			lat = append(lat, r.dur)
		} else {
			fails = append(fails, r)
		}
		time.Sleep(interval)
	}
}

func logf(t *testing.T, format string, a ...any) {
	t.Helper()
	t.Logf(format, a...)
}
