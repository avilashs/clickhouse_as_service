#!/usr/bin/env python3
"""Build a Pod from a live StatefulSet's pod template and break exactly one placement rule.

Usage: kubectl get sts X -o json | podvariant.py <variant> [arg]   -> Pod JSON on stdout

The pod keeps the StatefulSet ownerReference, so it is what the StatefulSet controller would
create; each variant then violates one rule of deploy/cell/policies/placement.yaml.
"""
import copy
import json
import sys


def required(spec):
    return spec["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]


def drop_term(spec, pred):
    terms = required(spec)
    terms[:] = [t for t in terms if not pred(t)]


def main():
    sts = json.load(sys.stdin)
    variant = sys.argv[1]
    arg = sys.argv[2] if len(sys.argv) > 2 else ""
    tmpl = copy.deepcopy(sts["spec"]["template"])
    meta = tmpl.get("metadata", {})
    pod = {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {
            "name": "e2e-variant-" + variant.replace("_", "-"),
            "namespace": sts["metadata"]["namespace"],
            "labels": meta.get("labels", {}),
            "ownerReferences": [{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": sts["metadata"]["name"],
                                 "uid": sts["metadata"]["uid"], "controller": True}],
        },
        "spec": tmpl["spec"],
    }
    spec = pod["spec"]
    # the StatefulSet controller adds one PVC volume per volumeClaimTemplate
    for vct in sts["spec"].get("volumeClaimTemplates", []):
        name = vct["metadata"]["name"]
        spec.setdefault("volumes", []).append(
            {"name": name, "persistentVolumeClaim": {"claimName": name + "-" + sts["metadata"]["name"] + "-0"}})
    c0 = spec["containers"][0]
    if variant == "good":
        pass
    elif variant == "no_sandbox":
        spec.pop("runtimeClassName", None)
    elif variant == "other_runtime":
        spec["runtimeClassName"] = "basalt-e2e-runc"  # created by test/e2e/m2.sh
    elif variant == "shared_dedicated_toleration":
        spec["tolerations"].append({"key": "basalt.io/dedicated-cluster", "operator": "Equal", "value": arg, "effect": "NoSchedule"})
    elif variant == "no_exclusivity":
        drop_term(spec, lambda t: "namespaceSelector" in t)
    elif variant == "scoped_exclusivity":
        for t in required(spec):
            if "namespaceSelector" in t:
                t["namespaceSelector"] = {"matchLabels": {"kubernetes.io/metadata.name": pod["metadata"]["namespace"]}}
    elif variant == "other_pool":
        spec["nodeSelector"]["basalt.io/dedicated-cluster"] = arg
    elif variant == "other_toleration":
        spec["tolerations"] = [{"key": "basalt.io/dedicated-cluster", "operator": "Equal", "value": arg, "effect": "NoSchedule"}]
    elif variant == "no_shard_antiaffinity":
        drop_term(spec, lambda t: "matchLabelKeys" in t)
    elif variant == "no_zone_spread":
        spec.pop("topologySpreadConstraints", None)
    elif variant == "sa_token":
        spec["automountServiceAccountToken"] = True
    elif variant == "overcommit":
        c0["resources"]["requests"]["memory"] = "1Gi"
    elif variant == "privileged":
        c0["securityContext"]["allowPrivilegeEscalation"] = True
    elif variant == "tolerate_all":
        spec["tolerations"].append({"operator": "Exists"})
    elif variant == "host_path":
        spec["volumes"].append({"name": "host", "hostPath": {"path": "/"}})
    else:
        sys.exit("unknown variant " + variant)
    json.dump(pod, sys.stdout)


if __name__ == "__main__":
    main()
