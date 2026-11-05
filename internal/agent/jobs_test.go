package agent

import (
	"strings"
	"testing"

	"github.com/avilashs/clickhouse_as_service/internal/model"
)

func TestScrubHidesTheSecretsInAnError(t *testing.T) {
	spec := model.JobSpec{SecretAccessKey: "sk-very-secret", SessionToken: "token-abc"}
	msg := scrub("S3 error: bad signature for sk-very-secret with token-abc", spec)
	if strings.Contains(msg, "sk-very-secret") || strings.Contains(msg, "token-abc") {
		t.Fatal(msg)
	}
	if got := scrub("no secrets here", model.JobSpec{}); got != "no secrets here" {
		t.Fatal(got)
	}
}

func TestSQLStringEscapes(t *testing.T) {
	if got := sqlString(`a'b\c`); got != `'a\'b\\c'` {
		t.Fatal(got)
	}
}

func TestCollectionNameIsAnIdentifier(t *testing.T) {
	if got := collectionName("backup-oabc123"); got != "basalt_job_backup_oabc123" {
		t.Fatal(got)
	}
}
