package hatSql

import (
	"strconv"
	"testing"
)

const (
	benchmarkM065LFrameRows       = 1024
	benchmarkM065LFramePartitions = 16
	benchmarkM065LFrameWidth      = 7
)

var benchmarkM065LFrameSink int

func BenchmarkM065LFrameMutation(b *testing.B) {
	seed := benchmarkM065LFrameRowsForMutation()
	update := Row{"id": "row-0", "partition": "partition-0", "order": int64(0), "value": int64(777)}
	b.Run("full_recompute", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var sink int
		for index := 0; index < b.N; index++ {
			rows := benchmarkM065LFrameApplyUpdate(seed, update)
			outputs := benchmarkM065LFrameRecompute(rows)
			sink += len(outputs)
		}
		benchmarkM065LFrameSink = sink
	})
	b.Run("mutable_update", func(b *testing.B) {
		window, err := NewMutableIncrementalFrameWindow(m065lFrameWindowDefinition(IncrementalWindowFrameSumInt64, benchmarkM065LFrameWidth))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := window.Apply(m065lInsertMutations(seed)); err != nil {
			b.Fatal(err)
		}
		updates := [2][]IncrementalFrameWindowMutation{
			{{Kind: IncrementalFrameWindowUpdate, Key: "row-0", Row: update}},
			{{Kind: IncrementalFrameWindowUpdate, Key: "row-0", Row: Row{"id": "row-0", "partition": "partition-0", "order": int64(0), "value": int64(778)}}},
		}
		b.ReportAllocs()
		b.ResetTimer()
		var sink int
		for index := 0; index < b.N; index++ {
			changes, err := window.Apply(updates[index&1])
			if err != nil {
				b.Fatal(err)
			}
			sink += len(changes)
		}
		benchmarkM065LFrameSink = sink
	})
}

func benchmarkM065LFrameRowsForMutation() []Row {
	rows := make([]Row, benchmarkM065LFrameRows)
	rowsPerPartition := benchmarkM065LFrameRows / benchmarkM065LFramePartitions
	for index := range rows {
		partition := index / rowsPerPartition
		rows[index] = Row{
			"id":        "row-" + strconv.Itoa(index),
			"partition": "partition-" + strconv.Itoa(partition),
			"order":     int64(index % rowsPerPartition),
			"value":     int64(index % 23),
		}
	}
	return rows
}

func benchmarkM065LFrameApplyUpdate(seed []Row, update Row) []Row {
	rows := make([]Row, len(seed))
	for index, row := range seed {
		if row["id"] == update["id"] {
			rows[index] = update
		} else {
			rows[index] = row
		}
	}
	return rows
}

func benchmarkM065LFrameRecompute(rows []Row) []Row {
	outputs := make([]Row, 0, len(rows))
	for start := 0; start < len(rows); {
		partition := rows[start]["partition"]
		end := start + 1
		for end < len(rows) && rows[end]["partition"] == partition {
			end++
		}
		for index := start; index < end; index++ {
			frameStart := index - benchmarkM065LFrameWidth
			if frameStart < start {
				frameStart = start
			}
			var sum int64
			for frameIndex := frameStart; frameIndex <= index; frameIndex++ {
				sum += rows[frameIndex]["value"].(int64)
			}
			output := make(Row, len(rows[index])+1)
			for key, value := range rows[index] {
				output[key] = value
			}
			output["frame_value"] = sum
			outputs = append(outputs, output)
		}
		start = end
	}
	return outputs
}
