package clickhouse

import (
	"fmt"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
)

// Observe turns the agent's report into a verdict. It deliberately does not trust the
// operator's "Completed" alone: on kind we saw the CHI report Completed before the first
// replica had loaded the updated remote_servers, so every replica must also answer SQL,
// see all its peers in system.clusters, and reach Keeper.
func (Engine) Observe(d engine.Desired, generation int64, obs *engine.Observed) engine.Health {
	if obs == nil {
		return engine.Health{Phase: engine.PhaseProgressing, Reasons: []string{"no report from the cell agent yet"}}
	}
	var reasons []string
	if obs.AppliedGeneration < generation {
		reasons = append(reasons, fmt.Sprintf("agent applied generation %d, want %d", obs.AppliedGeneration, generation))
	}
	if obs.Error != "" {
		reasons = append(reasons, "agent: "+obs.Error)
	}
	n := namesFor(d.ClusterID)
	for _, key := range []string{"ClickHouseKeeperInstallation/" + n.chk, "ClickHouseInstallation/" + n.chi} {
		if st := obs.Objects[key]; st != "Completed" {
			reasons = append(reasons, fmt.Sprintf("%s status %q", key, st))
		}
	}
	if d.Expect.PoolNodes > 0 && obs.PoolNodes < d.Expect.PoolNodes {
		reasons = append(reasons, fmt.Sprintf("node pool has %d/%d nodes", obs.PoolNodes, d.Expect.PoolNodes))
	}
	if obs.PodsReady < d.Expect.Pods {
		reasons = append(reasons, fmt.Sprintf("%d/%d pods ready", obs.PodsReady, d.Expect.Pods))
	}
	if obs.BootstrapHash != obs.AppliedHash || obs.AppliedHash == "" {
		reasons = append(reasons, "access bootstrap not applied for the current payload")
	}
	healthy := 0
	for _, p := range obs.Probes {
		switch {
		case !p.OK:
			reasons = append(reasons, fmt.Sprintf("%s: %s", p.Host, p.Error))
		case p.ClusterHosts != d.Expect.ServerHosts:
			reasons = append(reasons, fmt.Sprintf("%s sees %d/%d hosts in system.clusters", p.Host, p.ClusterHosts, d.Expect.ServerHosts))
		case !p.KeeperOK:
			reasons = append(reasons, fmt.Sprintf("%s cannot reach Keeper", p.Host))
		default:
			healthy++
		}
	}
	if len(obs.Probes) < d.Expect.ServerHosts {
		reasons = append(reasons, fmt.Sprintf("%d/%d replicas probed", len(obs.Probes), d.Expect.ServerHosts))
	}
	if len(reasons) == 0 {
		return engine.Health{Phase: engine.PhaseReady}
	}
	// Some replicas answer but not all: the cluster serves, with reduced redundancy.
	if healthy > 0 && obs.AppliedGeneration >= generation && obs.Objects["ClickHouseInstallation/"+n.chi] == "Completed" {
		return engine.Health{Phase: engine.PhaseDegraded, Reasons: reasons}
	}
	return engine.Health{Phase: engine.PhaseProgressing, Reasons: reasons}
}
