package hatriecache

import (
	"os"
	"strings"
	"testing"
)

func TestBenchmarkSerializationScriptIncludesDocumentedStructuredJournalBenches(t *testing.T) {
	data, err := os.ReadFile("scripts/benchmark-serialization.sh")
	if err != nil {
		t.Fatalf("ReadFile(benchmark-serialization.sh) error = %v", err)
	}
	script := string(data)
	for _, token := range []string{
		"CommandJournal(Encode|Decode)(Structured)?(JSON|Binary)",
		"SnapshotFormat(JSON|Binary|GzipJSON|GzipBestJSON|GzipBinary|GzipBestBinary|Structured",
		"LevelDB(Save(Materialized|MaterializedJSON|StructuredMaterialized",
	} {
		if !strings.Contains(script, token) {
			t.Fatalf("benchmark-serialization.sh default benchmark regex does not include %q", token)
		}
	}
}
