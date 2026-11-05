package clickhouse

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/avilashs/clickhouse_as_service/internal/engine"
	"github.com/avilashs/clickhouse_as_service/internal/model"
)

// ParseVersion splits "26.8.34.13" into its numbers.
func ParseVersion(v string) ([4]int, bool) {
	var out [4]int
	parts := strings.Split(v, ".")
	if len(parts) != 4 {
		return out, false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return out, false
		}
		out[i] = n
	}
	return out, true
}

// CompareVersions returns -1, 0 or 1.
func CompareVersions(a, b [4]int) int {
	for i := range a {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	return 0
}

// MajorMinor is "26.8" for "26.8.34.13".
func MajorMinor(v string) string {
	p := strings.SplitN(v, ".", 3)
	if len(p) < 2 {
		return v
	}
	return p[0] + "." + p[1]
}

// ValidateUpdate decides which changes of a live cluster are accepted. Anything not listed is
// refused with a reason rather than half-supported: M5 covers vertical scale, replica count and
// server upgrade, and each of those is exercised under load by the lifecycle tests.
func (e Engine) ValidateUpdate(cur, next model.ClusterSpec, cell model.Cell) []engine.FieldError {
	errs := e.Validate(next, cell)
	if len(errs) > 0 {
		return errs
	}
	bad := func(field, msg string) { errs = append(errs, engine.FieldError{Field: field, Message: msg}) }
	if next.Engine != cur.Engine {
		bad("engine", "cannot be changed")
	}
	if next.Tier != cur.Tier {
		bad("tier", "cannot be changed; migrate to the other tier with a restore")
	}
	if next.Shards != cur.Shards {
		bad("shards", "changing the shard count is not supported yet")
	}
	switch {
	case next.StorageGiB < cur.StorageGiB:
		bad("storageGiB", "volumes cannot shrink")
	case next.StorageGiB > cur.StorageGiB && cell.Profile == "local":
		bad("storageGiB", "this cell's volumes (local-path) cannot be expanded")
	}
	if cur.Tier == model.TierDedicated && next.Size != cur.Size {
		bad("size", "a dedicated cluster's node type cannot change yet (needs node rotation)")
	}
	if next.KeeperImageVersion() != cur.KeeperImageVersion() {
		bad("keeperVersion", "Keeper upgrades are not supported yet")
	}
	if next.ServerVersion != cur.ServerVersion {
		vc, ok1 := ParseVersion(cur.ServerVersion)
		vn, ok2 := ParseVersion(next.ServerVersion)
		switch {
		case !ok1 || !ok2:
			bad("version", fmt.Sprintf("%q is not a version like 26.8.34.13", next.ServerVersion))
		case CompareVersions(vn, vc) < 0:
			bad("version", "downgrades are not supported; a failed upgrade is rolled back automatically")
		}
	}
	return errs
}

// PlanUpgrade fills in the compatibility pin of a spec that changes the server version: when the
// minor version changes, the new binary keeps the old minor's defaults until the upgrade is
// finalised (design §13.1); a patch release needs no pin. finalize clears the pin.
func PlanUpgrade(cur, next model.ClusterSpec, finalize bool) model.ClusterSpec {
	switch {
	case finalize:
		next.CompatibilityPin = ""
	case next.ServerVersion != cur.ServerVersion && MajorMinor(next.ServerVersion) != MajorMinor(cur.ServerVersion):
		next.CompatibilityPin = MajorMinor(cur.ServerVersion)
	default:
		next.CompatibilityPin = cur.CompatibilityPin
	}
	return next
}
