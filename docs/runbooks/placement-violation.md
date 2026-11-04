# Runbook: BasaltPlacementViolation

**Alert:** `max by (rule, cluster) (basalt_placement_violations) > 0` for 2 minutes. Source: `basalt-audit` in `basalt-system` (design §8.4).

Admission (`deploy/cell/policies/placement.yaml`) stops new pods from breaking these rules. A violation therefore means one of four things:
- state changed after admission, such as a node relabelled or a taint removed by hand;
- a pod was created in a platform namespace, which admission allows;
- admission was bypassed or disabled;
- there is a bug in the rules.

## First steps

1. List the violations:

   ```bash
   kubectl -n basalt-system exec deploy/basalt-audit -- /basalt-audit --once -o json
   ```

2. Check the admission policies are installed and bound:

   ```bash
   kubectl get validatingadmissionpolicy,validatingadmissionpolicybinding | grep basalt
   ```

3. Check recent changes to nodes. Look at the audit logs for `nodes` updates by anything other than `basalt-agent` or the node lifecycle controller.

## By rule

| Rule | Meaning | Action |
|---|---|---|
| V1 | Two replicas of one shard on one node | This loses HA: one node failure takes out the shard. Delete one of the pods (`kubectl delete pod`), and the scheduler re-places it, respecting anti-affinity. If it lands on the same node, check the pod's anti-affinity term. |
| V1b | Replicas of a shard not spread over zones | Check whether zones lack capacity (`kubectl get nodes -L topology.kubernetes.io/zone,basalt.io/pool`). Add a node in the missing zone, then delete one replica pod so it reschedules. |
| V2 | Foreign pod on a dedicated node | **Security-relevant: tenant isolation.** Identify the pod and its creator from the audit log, delete it, and find out how it got there (platform namespace, toleration, manual `nodeName`). |
| V2b | Two ClickHouse servers on a dedicated node | **Isolation breach.** Same as V2. Check the exclusivity anti-affinity on both pods. |
| V3 | Shared server without the sandbox, or on the wrong pool; dedicated server off its pool | **Security-relevant.** Hostile SQL may be running outside the sandbox. Delete the pod; its StatefulSet recreates it through admission. Check the cell's `placement-params` and RuntimeClass. |
| V4 | Dedicated node count ≠ server count for more than 30 minutes | Usually a stuck node rotation (a surge node never removed) or a failed release. Check the agent logs for `node pool`. Release or reclaim nodes through the agent, never by hand-editing labels. |
| V5 | Keeper off the control pool | Delete the Keeper pod so it reschedules onto the control pool. Check the control pool's capacity. |
