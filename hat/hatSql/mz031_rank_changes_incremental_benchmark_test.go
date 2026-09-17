package hatSql

import (
	"sort"
	"testing"
)

type mz031RankBenchmarkChange struct {
	key        string
	score      int64
	beforeRank int
	afterRank  int
	diff       int64
}

func BenchmarkMZ031RankedTopKRebuildChangeBaseline(b *testing.B) {
	const (
		rows = 10000
		k    = 20
	)
	values := make([]mz031RankBenchmarkRow, rows)
	for index := range values {
		values[index] = mz031RankBenchmarkRow{key: "row-" + string(rune(index)), score: int64(index)}
	}
	ordered := make([]mz031RankBenchmarkRow, rows)
	previous := make([]mz031RankBenchmarkRow, k)
	copy(ordered, values)
	mz031SortRankBenchmarkRows(ordered)
	copy(previous, ordered[:k])
	changes := make([]mz031RankBenchmarkChange, 0, k*2)
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	var changeCount int64
	for iteration := 0; iteration < b.N; iteration++ {
		index := (iteration*7919 + 17) % rows
		values[index].score = int64((iteration*104729 + 23) % (rows * 4))
		copy(ordered, values)
		mz031SortRankBenchmarkRows(ordered)
		changes = changes[:0]
		for previousIndex, row := range previous {
			afterRank := 0
			score := row.score
			for currentIndex, current := range ordered[:k] {
				if current.key == row.key {
					afterRank = currentIndex + 1
					score = current.score
					break
				}
			}
			beforeRank := previousIndex + 1
			if beforeRank == afterRank {
				continue
			}
			diff := int64(0)
			if afterRank == 0 {
				diff = -1
			}
			changes = append(changes, mz031RankBenchmarkChange{
				key:        row.key,
				score:      score,
				beforeRank: beforeRank,
				afterRank:  afterRank,
				diff:       diff,
			})
		}
		for currentIndex, row := range ordered[:k] {
			found := false
			for _, previousRow := range previous {
				if previousRow.key == row.key {
					found = true
					break
				}
			}
			if found {
				continue
			}
			changes = append(changes, mz031RankBenchmarkChange{
				key:       row.key,
				score:     row.score,
				afterRank: currentIndex + 1,
				diff:      1,
			})
		}
		changeCount += int64(len(changes))
		for _, change := range changes {
			checksum += int64(len(change.key)) + int64(change.beforeRank) + int64(change.afterRank) + change.diff + change.score
		}
		copy(previous, ordered[:k])
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "checksum")
	b.ReportMetric(float64(changeCount)/float64(b.N), "changes/op")
}

func BenchmarkMZ031RankedTopKIncremental(b *testing.B) {
	const (
		rows = 10000
		k    = 20
	)
	keys := make([]string, rows)
	seed := make([]DifferentialRow, rows)
	for index := range seed {
		keys[index] = "row-" + string(rune(index))
		seed[index] = DifferentialRow{
			Key:  keys[index],
			Diff: 1,
			Row:  Row{"score": int64(index)},
		}
	}
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:          k,
		OrderKey:   c213TopKOrderKey,
		Descending: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := topK.Apply(seed); err != nil {
		b.Fatal(err)
	}
	updates := make([]DifferentialRow, 2)
	updateRow := Row{"score": int64(0)}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum int64
	var changeCount int64
	for iteration := 0; iteration < b.N; iteration++ {
		index := (iteration*7919 + 17) % rows
		updateRow["score"] = int64((iteration*104729 + 23) % (rows * 4))
		updates[0] = DifferentialRow{Key: keys[index], Diff: -1}
		updates[1] = DifferentialRow{Key: keys[index], Diff: 1, Row: updateRow}
		changes, err := topK.ApplyWithRankChanges(updates)
		if err != nil {
			b.Fatal(err)
		}
		changeCount += int64(len(changes))
		for _, change := range changes {
			score, _ := change.Row["score"].(int64)
			checksum += int64(len(change.Key)) + int64(change.BeforeRank) + int64(change.AfterRank) + change.Diff + score
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(checksum), "checksum")
	b.ReportMetric(float64(changeCount)/float64(b.N), "changes/op")
}

func mz031SortRankBenchmarkRows(rows []mz031RankBenchmarkRow) {
	sort.Slice(rows, func(left, right int) bool {
		if rows[left].score != rows[right].score {
			return rows[left].score > rows[right].score
		}
		return rows[left].key < rows[right].key
	})
}
