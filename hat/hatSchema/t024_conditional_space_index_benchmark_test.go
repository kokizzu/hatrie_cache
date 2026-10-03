package hatSchema

import "testing"

func BenchmarkT024ConditionalSpaceIndex(b *testing.B) {
	source := t024BenchmarkSource(b)
	if _, err := source.BuildFunctionalIndex("name_index", []string{"name"}, func(row Row) (interface{}, error) {
		return row["name"], nil
	}); err != nil {
		b.Fatal(err)
	}
	conditional := t024BenchmarkSource(b)
	if _, err := conditional.BuildConditionalFunctionalIndex(
		"active_name",
		[]string{"name", "active"},
		ConditionalFunctionalIndexOptions{
			Predicate: "active = true",
			Matches: func(row Row) (bool, error) {
				return row["active"].(bool), nil
			},
		},
		func(row Row) (interface{}, error) {
			return row["name"], nil
		},
	); err != nil {
		b.Fatal(err)
	}

	b.Run("conditional_lookup", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var count int
		for index := 0; index < b.N; index++ {
			count += len(conditional.Lookup("active_name", "tenant-040"))
		}
		b.StopTimer()
		if count == 0 {
			b.Fatal("conditional lookup returned no rows")
		}
	})
	b.Run("functional_lookup_then_filter", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var count int
		for index := 0; index < b.N; index++ {
			for _, row := range source.Lookup("name_index", "tenant-040") {
				if row["active"] == true {
					count++
				}
			}
		}
		b.StopTimer()
		if count == 0 {
			b.Fatal("filtered lookup returned no rows")
		}
	})
	b.Run("conditional_build", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, err := conditional.BuildConditionalFunctionalIndex(
				"active_name",
				[]string{"name", "active"},
				ConditionalFunctionalIndexOptions{
					Predicate: "active = true",
					Matches:   func(row Row) (bool, error) { return row["active"].(bool), nil },
				},
				func(row Row) (interface{}, error) { return row["name"], nil },
			); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("functional_build", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, err := source.BuildFunctionalIndex(
				"name_index",
				[]string{"name"},
				func(row Row) (interface{}, error) { return row["name"], nil },
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func t024BenchmarkSource(b *testing.B) *MaterializedSource {
	b.Helper()
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "name"}, {Name: "active"}})
	for index := 0; index < 50000; index++ {
		if _, err := source.Insert(Row{
			"id":     int64(index),
			"name":   "tenant-" + t024BenchmarkTenant(index%100),
			"active": index%7 == 0,
		}); err != nil {
			b.Fatal(err)
		}
	}
	return source
}

func t024BenchmarkTenant(value int) string {
	digits := [3]byte{'0', '0', '0'}
	digits[0] += byte(value / 100)
	digits[1] += byte(value / 10 % 10)
	digits[2] += byte(value % 10)
	return string(digits[:])
}
