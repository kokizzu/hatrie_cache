package hatSql

import (
	"sort"
	"strconv"
	"testing"
)

var benchmarkIncrementalOffsetWindowRowsSink []Row
var benchmarkIncrementalOffsetWindowUpdatesSink []DifferentialRow

func BenchmarkIncrementalOffsetWindowMaintenance(b *testing.B) {
	for _, direction := range []struct {
		name      string
		direction IncrementalOffsetWindowDirection
	}{
		{name: "lag", direction: IncrementalWindowLag},
		{name: "lead", direction: IncrementalWindowLead},
	} {
		definition := benchmarkIncrementalOffsetWindowDefinition(direction.direction)
		seed := benchmarkIncrementalOffsetWindowRows(1024)
		tail := Row{"id": "tail", "partition": "p00", "order": int64(64), "value": int64(64)}
		candidate := append(append([]Row(nil), seed...), tail)

		b.Run(direction.name+"_full_scan", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				benchmarkIncrementalOffsetWindowRowsSink = benchmarkFullOffsetWindow(candidate, definition)
			}
		})

		b.Run(direction.name+"_incremental", func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			window, err := NewIncrementalOffsetWindow(definition)
			if err != nil {
				b.Fatal(err)
			}
			window.keys = make(map[string]struct{}, len(seed)+b.N)
			if _, err := window.Append(seed); err != nil {
				b.Fatal(err)
			}
			tailKeys := make([]string, b.N)
			for index := range tailKeys {
				tailKeys[index] = "tail-" + strconv.Itoa(index)
			}
			tail := Row{"id": "", "partition": "p00", "order": int64(0), "value": int64(0)}
			batch := []Row{tail}
			b.StartTimer()
			for index := 0; index < b.N; index++ {
				tail["id"] = tailKeys[index]
				tail["order"] = int64(64 + index)
				tail["value"] = int64(1024 + index)
				updates, err := window.Append(batch)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkIncrementalOffsetWindowUpdatesSink = updates
			}
		})
	}
}

func benchmarkIncrementalOffsetWindowDefinition(direction IncrementalOffsetWindowDirection) IncrementalOffsetWindowDefinition {
	return IncrementalOffsetWindowDefinition{
		Direction:    direction,
		OutputColumn: "offset_value",
		Offset:       1,
		DefaultValue: int64(-1),
		PartitionKey: func(row Row) (string, error) { return row["partition"].(string), nil },
		OrderKey:     func(row Row) (interface{}, error) { return row["order"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
		ValueKey:     func(row Row) (interface{}, error) { return row["value"], nil },
	}
}

func benchmarkIncrementalOffsetWindowRows(count int) []Row {
	rows := make([]Row, 0, count)
	for index := 0; index < count; index++ {
		partition := index / 64
		order := index % 64
		rows = append(rows, Row{
			"id":        "row-" + string(rune('a'+partition)) + "-" + string(rune('a'+order)),
			"partition": "p" + string(rune('0'+partition/10)) + string(rune('0'+partition%10)),
			"order":     int64(order),
			"value":     int64(index),
		})
	}
	return rows
}

type benchmarkOffsetWindowRow struct {
	key       string
	row       Row
	partition string
	order     interface{}
	value     interface{}
}

func benchmarkFullOffsetWindow(rows []Row, definition IncrementalOffsetWindowDefinition) []Row {
	byPartition := make(map[string][]benchmarkOffsetWindowRow)
	partitionOrder := make([]string, 0)
	for _, row := range rows {
		partition, _ := definition.PartitionKey(row)
		order, _ := definition.OrderKey(row)
		key, _ := definition.RowKey(row)
		value, _ := definition.ValueKey(row)
		if _, exists := byPartition[partition]; !exists {
			partitionOrder = append(partitionOrder, partition)
		}
		byPartition[partition] = append(byPartition[partition], benchmarkOffsetWindowRow{
			key: key, row: row, partition: partition, order: order, value: value,
		})
	}
	output := make([]Row, 0, len(rows))
	for _, partition := range partitionOrder {
		partitionRows := byPartition[partition]
		sort.SliceStable(partitionRows, func(left, right int) bool {
			comparison := sqlCompare(partitionRows[left].order, partitionRows[right].order)
			if comparison != 0 {
				return comparison < 0
			}
			return partitionRows[left].key < partitionRows[right].key
		})
		for index, row := range partitionRows {
			value := definition.DefaultValue
			if definition.Direction == IncrementalWindowLag {
				if index >= definition.Offset {
					value = partitionRows[index-definition.Offset].value
				}
			} else if index+definition.Offset < len(partitionRows) {
				value = partitionRows[index+definition.Offset].value
			}
			output = append(output, incrementalOffsetWindowOutput(row.row, definition.OutputColumn, value))
		}
	}
	return output
}
