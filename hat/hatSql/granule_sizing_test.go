package hatSql_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestGranuleSizingPolicyAdaptsToObservedSelectivity(t *testing.T) {
	policy, err := hatSql.NewGranuleSizingPolicy(hatSql.GranuleSizingOptions{
		MinRows:           256,
		DefaultRows:       1024,
		MaxRows:           8192,
		TargetSelectivity: 0.10,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		current int
		scanned uint64
		matched uint64
		want    int
	}{
		{name: "no matches", current: 1024, scanned: 1000, matched: 0, want: 256},
		{name: "target", current: 1024, scanned: 1000, matched: 100, want: 1024},
		{name: "selective", current: 1024, scanned: 1000, matched: 50, want: 512},
		{name: "dense", current: 1024, scanned: 1000, matched: 500, want: 5120},
		{name: "all matches", current: 1024, scanned: 1000, matched: 1000, want: 8192},
		{name: "zero observation", current: 1024, scanned: 0, matched: 0, want: 1024},
		{name: "invalid observation", current: 1024, scanned: 100, matched: 101, want: 1024},
		{name: "current below minimum", current: 1, scanned: 1000, matched: 100, want: 256},
		{name: "current above maximum", current: 10000, scanned: 1000, matched: 100, want: 8192},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := policy.Suggest(test.current, test.scanned, test.matched); got != test.want {
				t.Fatalf("Suggest() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestGranuleSizingPolicyValidatesOptions(t *testing.T) {
	for name, options := range map[string]hatSql.GranuleSizingOptions{
		"negative minimum":      {MinRows: -1, DefaultRows: 1024, MaxRows: 8192, TargetSelectivity: 0.1},
		"default below minimum": {MinRows: 1024, DefaultRows: 256, MaxRows: 8192, TargetSelectivity: 0.1},
		"maximum below minimum": {MinRows: 1024, DefaultRows: 2048, MaxRows: 256, TargetSelectivity: 0.1},
		"negative target":       {MinRows: 256, DefaultRows: 1024, MaxRows: 8192, TargetSelectivity: -0.1},
		"target above one":      {MinRows: 256, DefaultRows: 1024, MaxRows: 8192, TargetSelectivity: 1.1},
	} {
		if _, err := hatSql.NewGranuleSizingPolicy(options); !errors.Is(err, hatSql.ErrGranuleSizingOptionsInvalid) {
			t.Errorf("%s error = %v", name, err)
		}
	}
}

func BenchmarkGranuleSizingPolicySuggest(b *testing.B) {
	policy, err := hatSql.NewGranuleSizingPolicy(hatSql.GranuleSizingOptions{
		MinRows:           256,
		DefaultRows:       1024,
		MaxRows:           8192,
		TargetSelectivity: 0.10,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if got := policy.Suggest(1024, 1000, 50); got == 0 {
			b.Fatal("suggested granule size is zero")
		}
	}
}
