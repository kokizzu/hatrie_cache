package hatriecache

import (
	"os"
	"strings"
	"testing"
)

func TestMonitoringWrapperDefersFormatDefaultsToGoConfig(t *testing.T) {
	makefile, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("ReadFile(Makefile) error = %v", err)
	}
	script, err := os.ReadFile("scripts/monitoring-server.sh")
	if err != nil {
		t.Fatalf("ReadFile(scripts/monitoring-server.sh) error = %v", err)
	}

	makefileText := string(makefile)
	for _, token := range []string{
		"REPLICATION_WIRE_FORMAT ?=\n",
		"DB_FORMAT ?=\n",
		"SNAPSHOT_FORMAT ?=\n",
		"JOURNAL_FORMAT ?=\n",
	} {
		if !strings.Contains(makefileText, token) {
			t.Fatalf("Makefile should leave format override empty by default; missing %q", token)
		}
	}

	scriptText := string(script)
	for _, token := range []string{
		"replication_wire_format=${REPLICATION_WIRE_FORMAT:-}",
		"db_format=${DB_FORMAT:-}",
		"snapshot_format=${SNAPSHOT_FORMAT:-}",
		"journal_format=${JOURNAL_FORMAT:-}",
		`set -- "$@" -replication-wire-format "$replication_wire_format"`,
		`set -- "$@" -db-format "$db_format"`,
		`set -- "$@" -snapshot-format "$snapshot_format"`,
		`set -- "$@" -journal-format "$journal_format"`,
	} {
		if !strings.Contains(scriptText, token) {
			t.Fatalf("monitoring wrapper should pass explicit format overrides only; missing %q", token)
		}
	}
	for _, staleDefault := range []string{
		"REPLICATION_WIRE_FORMAT:-protobuf",
		"DB_FORMAT:-binary",
		"SNAPSHOT_FORMAT:-gzip-best-binary",
		"JOURNAL_FORMAT:-binary",
	} {
		if strings.Contains(scriptText, staleDefault) {
			t.Fatalf("monitoring wrapper still duplicates Go default %q", staleDefault)
		}
	}
}
