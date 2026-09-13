package hatCache

import "testing"

func BenchmarkPebblePropertiesBaseline(b *testing.B) {
	store, err := OpenPebbleStore(b.TempDir())
	if err != nil {
		b.Fatalf("OpenPebbleStore() error = %v", err)
	}
	defer store.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := store.Properties(); err != nil {
			b.Fatalf("Properties() error = %v", err)
		}
	}
}
