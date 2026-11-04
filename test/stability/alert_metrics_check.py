#!/usr/bin/env python3
"""Check that every ClickHouse metric named in deploy/cell/monitoring/stability-alerts.yaml is
exported by the operator's metrics-exporter, and that every label the rules select on appears on
that metric. Reads a scrape of the exporter (default: port-forwards it from the cell).

This validates names and labels only. It does not evaluate the rules: promtool is not installed
in the M4 environment, so the thresholds and `for:` durations are unit-tested nowhere yet.

Usage: test/stability/alert_metrics_check.py [scrape.txt]
"""
import os, re, subprocess, sys, time
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RULES = os.path.join(ROOT, "deploy/cell/monitoring/stability-alerts.yaml")
# not exported by the operator: kube-state-metrics, listed here so a typo in them still shows up
EXTERNAL = {"kube_pod_container_status_last_terminated_reason"}

def scrape():
    if len(sys.argv) > 1:
        return open(sys.argv[1]).read()
    pf = subprocess.Popen(["kubectl", "-n", "kube-system", "port-forward", "svc/clickhouse-operator-metrics", "18888:8888"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(3)
        return subprocess.run(["curl", "-s", "-m", "20", "localhost:18888/metrics"], capture_output=True, text=True).stdout
    finally:
        pf.terminate()

text = scrape()
series = {}  # metric -> set of label names seen
for line in text.splitlines():
    m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})? ", line)
    if m and not line.startswith("#"):
        series.setdefault(m.group(1), set()).update(re.findall(r'(\w+)="', m.group(3) or ""))

rules = yaml.safe_load(open(RULES))
bad, n = [], 0
for g in rules["groups"]:
    for r in g["rules"]:
        expr = r["expr"]
        for sel in re.finditer(r"([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?", expr):
            name = sel.group(1)
            if not (name.startswith("chi_clickhouse_") or name in EXTERNAL):
                continue
            n += 1
            if name in EXTERNAL:
                continue
            if name not in series:
                bad.append(f"{r['alert']}: metric {name} is not exported")
                continue
            for lab in re.findall(r"(\w+)\s*=~?\s*\"", sel.group(3) or ""):
                if lab not in series[name]:
                    bad.append(f"{r['alert']}: {name} has no label {lab} (has {sorted(series[name])})")
        for key in ("severity",):
            if key not in r.get("labels", {}):
                bad.append(f"{r['alert']}: no {key} label")
        if not r.get("annotations", {}).get("summary"):
            bad.append(f"{r['alert']}: no summary")
        rb = r.get("annotations", {}).get("runbook")
        if rb and not os.path.exists(os.path.join(ROOT, rb)):
            bad.append(f"{r['alert']}: runbook {rb} does not exist")
print(f"{len(rules['groups'][0]['rules'])} alerts, {n} metric selectors checked")
for b in bad: print("FAIL  " + b)
print("== %d problems" % len(bad))
sys.exit(1 if bad else 0)
