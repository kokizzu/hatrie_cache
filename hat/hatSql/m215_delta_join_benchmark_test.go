package hatSql

import (
	"fmt"
	"testing"
)

func BenchmarkM215DeltaJoinHighChurnBaseline(b *testing.B) {
	join, updates := m215HighChurnJoinFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	var outputRows int64
	for iteration := 0; iteration < b.N; iteration++ {
		output, err := join.Apply(updates)
		if err != nil {
			b.Fatal(err)
		}
		outputRows += int64(len(output))
	}
	b.StopTimer()
	b.ReportMetric(float64(outputRows), "output_rows")
}

func BenchmarkM215DeltaJoinHighChurnConsolidated(b *testing.B) {
	join, updates := m215HighChurnJoinFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	var outputRows int64
	for iteration := 0; iteration < b.N; iteration++ {
		output, err := join.ApplyConsolidated(updates)
		if err != nil {
			b.Fatal(err)
		}
		outputRows += int64(len(output))
	}
	b.StopTimer()
	b.ReportMetric(float64(outputRows), "output_rows")
}

func m215HighChurnJoinFixture(b testing.TB) (*IncrementalJoin, []IncrementalJoinUpdate) {
	b.Helper()
	join, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  m215JoinKey,
		RightKey: m215JoinKey,
		Merge:    m215JoinMerge,
	})
	if err != nil {
		b.Fatal(err)
	}
	seed := make([]IncrementalJoinUpdate, 0, 513)
	for index := 0; index < 512; index++ {
		key := fmt.Sprintf("right-%04d", index)
		seed = append(seed, IncrementalJoinUpdate{
			Side: IncrementalJoinRight,
			Row: DifferentialRow{
				Key:  key,
				Diff: 1,
				Row:  Row{"id": key, "group": "hot"},
			},
		})
	}
	seed = append(seed, IncrementalJoinUpdate{
		Side: IncrementalJoinLeft,
		Row: DifferentialRow{
			Key:  "left-hot",
			Diff: 1,
			Row:  Row{"id": "left-hot", "group": "hot"},
		},
	})
	if _, err := join.Apply(seed); err != nil {
		b.Fatal(err)
	}
	updates := make([]IncrementalJoinUpdate, 0, 16)
	for index := 0; index < 8; index++ {
		updates = append(updates,
			IncrementalJoinUpdate{
				Side: IncrementalJoinLeft,
				Row:  DifferentialRow{Key: "left-hot", Diff: -1},
			},
			IncrementalJoinUpdate{
				Side: IncrementalJoinLeft,
				Row: DifferentialRow{
					Key:  "left-hot",
					Diff: 1,
					Row:  Row{"id": "left-hot", "group": "hot"},
				},
			},
		)
	}
	return join, updates
}

func m215JoinKey(row Row) (string, error) {
	group, ok := row["group"].(string)
	if !ok || group == "" {
		return "", fmt.Errorf("join group is required")
	}
	return group, nil
}

func m215JoinMerge(left, right Row) (Row, error) {
	return Row{"left_id": left["id"], "right_id": right["id"], "group": left["group"]}, nil
}
