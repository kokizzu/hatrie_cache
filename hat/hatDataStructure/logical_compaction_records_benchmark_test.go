package hatDataStructure

import "testing"

func BenchmarkLogicalCompactionRecords(b *testing.B) {
	compaction := logicalCompactionRecordsFixture()
	b.ReportAllocs()
	b.Run("records", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			logicalCompactionRecordsSink = compaction.Records()
		}
	})
	b.Run("records_into", func(b *testing.B) {
		destination := make([]DifferentialRecord[int], 0, compaction.Len())
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			destination = compaction.RecordsInto(destination)
			logicalCompactionRecordsIntoSink = destination
		}
	})
}

var logicalCompactionRecordsSink []DifferentialRecord[int]
var logicalCompactionRecordsIntoSink []DifferentialRecord[int]

func logicalCompactionRecordsFixture() *LogicalCompaction[int] {
	compaction := NewLogicalCompaction[int]()
	for index := 0; index < 4096; index++ {
		if err := compaction.Add(index, uint64(4095-index), 1); err != nil {
			panic(err)
		}
	}
	return compaction
}
