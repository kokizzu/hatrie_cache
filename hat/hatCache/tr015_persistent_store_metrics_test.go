package hatCache

import (
	"testing"
)

func TestPebblePropertiesExposeFilterAndReadAmplificationMetrics(t *testing.T) {
	store, err := OpenPebbleStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleStore() error = %v", err)
	}
	defer store.Close()

	properties, err := store.Properties()
	if err != nil {
		t.Fatalf("Properties() error = %v", err)
	}
	metrics := store.db.Metrics()
	if properties.ReadAmplification != metrics.ReadAmp() {
		t.Fatalf("read amplification = %d, want %d", properties.ReadAmplification, metrics.ReadAmp())
	}
	if properties.FilterHits != metrics.Filter.Hits || properties.FilterMisses != metrics.Filter.Misses {
		t.Fatalf("filter metrics = %d/%d, want %d/%d", properties.FilterHits, properties.FilterMisses, metrics.Filter.Hits, metrics.Filter.Misses)
	}
}

func TestLevelDBPropertiesLeavePebbleMetricsZero(t *testing.T) {
	store, err := OpenLevelDBStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()

	properties, err := store.Properties()
	if err != nil {
		t.Fatalf("Properties() error = %v", err)
	}
	if properties.ReadAmplification != 0 || properties.FilterHits != 0 || properties.FilterMisses != 0 {
		t.Fatalf("LevelDB-only metrics = %d/%d/%d, want zero", properties.ReadAmplification, properties.FilterHits, properties.FilterMisses)
	}
}
