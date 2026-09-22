package hatSql

import "testing"

func BenchmarkM222MaterializedComputeReplicaRefresh(b *testing.B) {
	for _, replicaCount := range []int{1, 2} {
		b.Run("replicas-"+string(rune('0'+replicaCount)), func(b *testing.B) {
			source := &m222MutableSource{rows: m222BenchmarkRows(256)}
			views := make([]MaterializedViewComputeReplica, 0, replicaCount)
			for index := 0; index < replicaCount; index++ {
				views = append(views, MaterializedViewComputeReplica{
					Name:  "replica-" + string(rune('a'+index)),
					Views: m222CreateReplicaViews(b, source),
				})
			}
			set, err := NewMaterializedViewComputeReplicaSet(views...)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := set.RefreshChanged(b.Context(), []string{"people"}, source, QueryOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func m222BenchmarkRows(count int) []Row {
	rows := make([]Row, count)
	for index := range rows {
		rows[index] = Row{
			"id":   int64(index),
			"name": "person",
		}
	}
	return rows
}
