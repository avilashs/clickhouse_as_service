#!/usr/bin/env python3
"""Boot a stock ClickHouse with the rendered settings and check what the server really does.

For each golden render (t-2 shared, dt-1-2x2 dedicated) this converts the CHI's server settings
and profiles to XML, merges them over the release's stock config.xml, starts a server on
loopback, and asserts:

  1. the server boots (design F-23: a bad rendered config is found in CI, not in a tenant's pod);
  2. the effective server settings, MergeTree defaults and default-profile settings equal the
     rendered ones;
  3. every system log table the server creates has a TTL, and the TTL we asked for;
  4. a profile constraint really refuses an out-of-range value.

Settings that need the cluster (TLS ports and certificates, Keeper, replicated access storage)
are left out; the live cluster tests cover them. Needs the `clickhouse` binary of the pinned
release on PATH. Usage: test/stability/render_boot.py
"""
import os, re, shutil, subprocess, sys, tempfile, time, urllib.request
import xml.etree.ElementTree as ET
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GOLDEN = os.path.join(ROOT, "internal/engine/clickhouse/testdata")
OPERATOR_D = os.path.join(ROOT, "test/stability/operator-config.d")
OPERATOR_USERS_D = os.path.join(ROOT, "test/stability/operator-users.d")
VERSION = "26.8.34.13"
STOCK_URL = f"https://raw.githubusercontent.com/ClickHouse/ClickHouse/v{VERSION}-lts/programs/server/config.xml"
CACHE = os.path.join(os.environ.get("BASALT_HOME", "/var/lib/basalt"), f"stock-config-{VERSION}.xml")
# need certificates, Keeper or other cluster pieces
SKIP_KEYS = re.compile(r"^(https_port|tcp_port_secure|interserver_https_port|(http|tcp|interserver_http|mysql|postgresql)_port)$")

fails = []
def check(name, ok, detail=""):
    print(("PASS  " if ok else "FAIL  ") + name + ("" if ok else f"   {detail}"))
    if not ok:
        fails.append(name)

def put(root, path, value):
    node = root
    for part in path.split("/"):
        nxt = node.find(part)
        node = nxt if nxt is not None else ET.SubElement(node, part)
    node.text = "" if value == "" else str(value).lower() if isinstance(value, bool) else str(value)

def to_xml(settings, wrap):
    root = ET.Element("clickhouse")
    top = ET.SubElement(root, wrap) if wrap else root
    for k, v in settings.items():
        if isinstance(v, str) and v == "_removed_" or SKIP_KEYS.match(k) or isinstance(v, (list, dict)):
            continue
        put(top, k, v)
    return ET.tostring(root, encoding="unicode")

def chi_of(desired):
    return next(o for o in desired["objects"] if o["kind"] == "ClickHouseInstallation")["spec"]["configuration"]

def main():
    if shutil.which("clickhouse") is None:
        print("SKIP  no clickhouse binary on PATH"); return 0
    ver = subprocess.run(["clickhouse", "local", "-q", "select version()"], capture_output=True, text=True).stdout.strip()
    if not ver.startswith(VERSION[:4]):
        print(f"SKIP  clickhouse is {ver}, the render targets {VERSION}"); return 0
    if not os.path.exists(CACHE):
        os.makedirs(os.path.dirname(CACHE), exist_ok=True)
        urllib.request.urlretrieve(STOCK_URL, CACHE)

    for golden in ("t-2", "dt-1-2x2"):
        print(f"== {golden}")
        cfg = chi_of(yaml.safe_load(open(os.path.join(GOLDEN, golden + ".golden.yaml"))))
        settings, profiles = cfg["settings"], cfg["profiles"]
        work = tempfile.mkdtemp(prefix="basalt-boot-")
        run = os.path.join(work, "run"); os.makedirs(os.path.join(run, "config.d")); os.makedirs(os.path.join(work, "data/tmp"))
        shutil.copy(CACHE, os.path.join(run, "config.xml"))
        # what the operator writes into every pod before our settings (see operator-config.d/README.md)
        for f in sorted(os.listdir(OPERATOR_D)):
            if f.endswith(".xml"): shutil.copy(os.path.join(OPERATOR_D, f), os.path.join(run, "config.d", f))
        open(os.path.join(run, "config.d/chop-generated-settings.xml"), "w").write(to_xml(settings, None))
        os.makedirs(os.path.join(run, "users.d"))
        for f in sorted(os.listdir(OPERATOR_USERS_D)):
            if f.endswith(".xml"): shutil.copy(os.path.join(OPERATOR_USERS_D, f), os.path.join(run, "users.d", f))
        # the operator's own user, in the profile its file defines (log_queries = 0, ...)
        open(os.path.join(run, "users.xml"), "w").write(
            "<clickhouse>" + to_xml(profiles, "profiles")[len("<clickhouse>"):-len("</clickhouse>")] +
            "<users><default><password></password><networks><ip>127.0.0.1</ip></networks><profile>default</profile></default>"
            "<clickhouse_operator><password>op</password><networks><ip>127.0.0.1</ip></networks><profile>clickhouse_operator</profile></clickhouse_operator>"
            "</users></clickhouse>")
        log = open(os.path.join(work, "server.log"), "w")
        proc = subprocess.Popen(
            ["clickhouse", "server", "--config-file", os.path.join(run, "config.xml"), "--",
             "--http_port=18123", "--tcp_port=19000", f"--path={work}/data/", f"--tmp_path={work}/data/tmp/",
             f"--user_files_path={work}/data/uf/", "--listen_host=127.0.0.1", "--logger.console=1", "--logger.level=warning"],
            cwd=run, stdout=log, stderr=log)
        def q(sql):
            return subprocess.run(["clickhouse", "client", "--port", "19000", "-q", sql], capture_output=True, text=True)
        try:
            up = False
            for _ in range(90):
                if proc.poll() is not None: break
                if q("select 1").returncode == 0: up = True; break
                time.sleep(1)
            first_error = next((l for l in open(os.path.join(work, "server.log")).read().splitlines() if "<Error>" in l and "Exception" in l), "")
            check("server boots with the rendered settings and profiles", up, first_error[:700])
            if not up: continue

            def server_setting(name):
                return q(f"select value from system.server_settings where name='{name}'").stdout.strip()
            for k in ("background_pool_size", "background_fetches_pool_size", "background_schedule_pool_size",
                      "concurrent_threads_soft_limit_num", "max_table_num_to_throw", "max_database_num_to_throw",
                      "max_table_size_to_drop", "max_partition_size_to_drop"):
                check(f"server setting {k} = {settings[k]}", server_setting(k) == str(settings[k]), server_setting(k))
            for k in ("parts_to_delay_insert", "parts_to_throw_insert", "max_parts_in_total"):
                got = q(f"select value from system.merge_tree_settings where name='{k}'").stdout.strip()
                check(f"MergeTree default {k} = {settings['merge_tree/' + k]}", got == str(settings["merge_tree/" + k]), got)
            for k in ("max_threads", "max_memory_usage", "max_partitions_per_insert_block", "query_profiler_real_time_period_ns",
                      "max_temporary_data_on_disk_size_for_user", "log_queries"):
                got = q(f"select value from system.settings where name='{k}'").stdout.strip()
                check(f"default profile {k} = {profiles['default/' + k]}", got == str(profiles["default/" + k]), got)
            r = q("select 1 settings max_partitions_per_insert_block = 100000")
            check("profile constraint refuses max_partitions_per_insert_block = 100000", "SETTING_CONSTRAINT_VIOLATION" in r.stderr, r.stderr[:200])
            r = q("select 1 settings log_queries = 0")
            check("profile constraint pins log_queries for tenants", "SETTING_CONSTRAINT_VIOLATION" in r.stderr, r.stderr[:200])
            # The operator connects over HTTP as clickhouse_operator, whose own profile sets
            # log_queries = 0. M4 found a `const` constraint on log_queries locking it out of every
            # server, so the check uses the same transport, with and without the URL parameter.
            import base64, urllib.error
            def http_as_operator(extra=""):
                req = urllib.request.Request("http://127.0.0.1:18123/?query=select%20version()" + extra)
                req.add_header("Authorization", "Basic " + base64.b64encode(b"clickhouse_operator:op").decode())
                try:
                    return urllib.request.urlopen(req, timeout=20).status, ""
                except urllib.error.HTTPError as e:
                    return e.code, e.read().decode()[:200]
            for extra in ("", "&log_queries=0", "&log_queries=1"):
                code, body = http_as_operator(extra)
                check(f"the operator's HTTP login works (URL parameters '{extra}')", code == 200, f"{code} {body}")

            # log tables: run some work, flush, and look at every *_log table the server created
            q("select count() from numbers(1000) format Null"); time.sleep(8); q("system flush logs")
            rows = q("select name, extract(engine_full, 'TTL event_date \\\\+ [^ ]+ ?[^ ]*') from system.tables "
                     "where database = 'system' and name like '%\\\\_log' and name != 'user_query_log' order by name format TSV").stdout.strip().splitlines()
            created = {r.split("\t")[0]: (r.split("\t") + [""])[1] for r in rows}
            check("the server created the system log tables", len(created) >= 20, str(sorted(created)))
            check("the operator's removal of query_thread_log holds", "query_thread_log" not in created, str(sorted(created)))
            for name in sorted(created):
                want = settings.get(name + "/ttl") or settings.get(name + "/engine")
                days = re.search(r"INTERVAL (\d+) DAY", want or "", re.I)
                got = q(f"select engine_full from system.tables where database='system' and name='{name}'").stdout
                ok = bool(days) and re.search(rf"TTL (event|finish)_date \+ toIntervalDay\({days.group(1)}\)", got) is not None
                if name == "query_thread_log": ok = False
                check(f"system.{name} has the rendered TTL ({days.group(1) if days else '?'} days)", ok, got[-160:])
            extra = sorted(t.rsplit("/", 1)[0] for t in settings if re.search(r"_log/(ttl|engine)$", t) and t.rsplit("/", 1)[0] not in created)
            check("no rendered TTL names a log table the server does not create", not extra, str(extra))
        finally:
            proc.terminate()
            try: proc.wait(30)
            except subprocess.TimeoutExpired: proc.kill()
            shutil.rmtree(work, ignore_errors=True)
    print(f"== {len(fails)} failed")
    return 1 if fails else 0

sys.exit(main())
