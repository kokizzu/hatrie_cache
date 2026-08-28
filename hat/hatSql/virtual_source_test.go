package hatSql

import "testing"

func TestVirtualSourcesExposeReadOnlySnapshots(t *testing.T) {
	sources := NewVirtualSources()
	if err := sources.Register("metrics", VirtualSourceFunc(func() ([]Row, error) { return []Row{{"name": "requests", "value": 7}}, nil })); err != nil {
		t.Fatal(err)
	}
	rows, err := sources.ResolveSQLVirtualSource("metrics")
	if err != nil || len(rows) != 1 || rows[0]["value"] != 7 {
		t.Fatalf("virtual source = %#v, %v", rows, err)
	}
	rows[0]["value"] = 9
	again, _ := sources.ResolveSQLVirtualSource("metrics")
	if again[0]["value"] != 7 {
		t.Fatalf("virtual source leaked mutation: %#v", again)
	}
}
