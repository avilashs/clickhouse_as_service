package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
	"github.com/avilashs/clickhouse_as_service/internal/store"
)

// staleObservation: an agent that has not reported for this long tells us nothing, so the
// cluster's state is left alone (design F-12: a partitioned cell never causes changes).
const staleObservation = 2 * time.Minute

// SyncHealth moves every ACTIVE or DEGRADED cluster to the state its latest observation implies
// (design F-5: a cluster that lost Keeper quorum is DEGRADED while its replicas serve reads). It
// returns the number of clusters whose state changed. Only ACTIVE and DEGRADED rows are touched,
// and only by a conditional update, so it never overwrites a workflow's CREATING, DELETING or
// FAILED.
func SyncHealth(ctx context.Context, st *store.Store, engines map[string]engine.Engine, now time.Time, log *slog.Logger) (int, error) {
	clusters, err := st.ListServingClusters(ctx)
	if err != nil {
		return 0, err
	}
	changed := 0
	for _, c := range clusters {
		e, ok := engines[c.Spec.Engine]
		if !ok {
			continue
		}
		obs, err := st.GetObserved(ctx, c.ID)
		if err != nil || obs == nil || now.Sub(obs.ObservedAt) > staleObservation {
			continue
		}
		row, err := st.GetDesired(ctx, c.ID)
		if err != nil || row.Tombstone {
			continue
		}
		var d engine.Desired
		if err := json.Unmarshal(row.Payload, &d); err != nil {
			log.Warn("health sync: desired payload", "cluster", c.ID, "err", err)
			continue
		}
		h := e.Observe(d, row.Generation, obs)
		state, reason := model.ClusterActive, ""
		if h.Phase != engine.PhaseReady {
			state, reason = model.ClusterDegraded, truncate(strings.Join(h.Reasons, "; "), 300)
			if reason == "" {
				reason = fmt.Sprintf("health is %s", h.Phase)
			}
		}
		did, err := st.SetServingHealth(ctx, c.ID, state, reason)
		if err != nil {
			log.Warn("health sync", "cluster", c.ID, "err", err)
			continue
		}
		if did {
			changed++
			log.Info("cluster health changed", "cluster", c.ID, "state", state, "reason", reason)
		}
	}
	return changed, nil
}
