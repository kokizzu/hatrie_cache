package hatDataStructure_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func BenchmarkMZ005MutableMapLookup(b *testing.B) {
	records := mz005BenchmarkRecords()
	values := make(map[string][]byte, len(records))
	for _, record := range records {
		values[record.Key] = record.Value
	}
	keys := mz005BenchmarkKeys(records)
	b.ReportAllocs()
	b.ResetTimer()
	var checksum byte
	for index := 0; index < b.N; index++ {
		value := append([]byte(nil), values[keys[index%len(keys)]]...)
		checksum ^= value[index%len(value)]
	}
	b.StopTimer()
	if checksum == 0xff {
		b.Fatal("unexpected checksum")
	}
}

func BenchmarkMZ005SealedRunLookup(b *testing.B) {
	benchmarkMZ005SealedRunLookup(b, 0)
}

func BenchmarkMZ005SealedRunLookupStride8(b *testing.B) {
	benchmarkMZ005SealedRunLookup(b, 8)
}

func BenchmarkMZ005SealedRunLookupStride16(b *testing.B) {
	benchmarkMZ005SealedRunLookup(b, 16)
}

func benchmarkMZ005SealedRunLookup(b *testing.B, stride int) {
	records := mz005BenchmarkRecords()
	options := hatDataStructure.SealedUpsertRunOptions{}
	if stride > 0 {
		options.IndexStride = stride
	}
	run, err := hatDataStructure.NewSealedUpsertRun(records, options)
	if err != nil {
		b.Fatal(err)
	}
	keys := mz005BenchmarkKeys(records)
	b.ReportAllocs()
	b.ResetTimer()
	var checksum byte
	for index := 0; index < b.N; index++ {
		record, found := run.Lookup(keys[index%len(keys)])
		if !found {
			b.Fatal("missing sealed record")
		}
		checksum ^= record.Value[index%len(record.Value)]
	}
	b.StopTimer()
	if checksum == 0xff {
		b.Fatal("unexpected checksum")
	}
}

func BenchmarkMZ005MutableMapBuild(b *testing.B) {
	records := mz005BenchmarkRecords()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		values := make(map[string][]byte, len(records))
		for _, record := range records {
			values[record.Key] = record.Value
		}
		if len(values) != len(records) {
			b.Fatal("unexpected map size")
		}
	}
}

func BenchmarkMZ005SealedRunBuild(b *testing.B) {
	records := mz005BenchmarkRecords()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		run, err := hatDataStructure.NewSealedUpsertRun(records, hatDataStructure.SealedUpsertRunOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if run.Len() != len(records) {
			b.Fatal("unexpected sealed run size")
		}
	}
}

func BenchmarkMZ005JSONMarshal(b *testing.B) {
	records := mz005BenchmarkRecords()
	jsonWire, err := json.Marshal(records)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := json.Marshal(records); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(jsonWire)), "wire-bytes/op")
}

func BenchmarkMZ005SealedRunMarshal(b *testing.B) {
	run, err := hatDataStructure.NewSealedUpsertRun(mz005BenchmarkRecords(), hatDataStructure.SealedUpsertRunOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := run.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(run.WireBytes()), "wire-bytes/op")
}

func BenchmarkMZ005JSONUnmarshal(b *testing.B) {
	records := mz005BenchmarkRecords()
	wire, err := json.Marshal(records)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var decoded []hatDataStructure.UpsertRecord[[]byte]
		if err := json.Unmarshal(wire, &decoded); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(wire)), "wire-bytes/op")
}

func BenchmarkMZ005SealedRunUnmarshal(b *testing.B) {
	run, err := hatDataStructure.NewSealedUpsertRun(mz005BenchmarkRecords(), hatDataStructure.SealedUpsertRunOptions{})
	if err != nil {
		b.Fatal(err)
	}
	wire, err := run.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	recordCount := run.Len()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decoded, err := hatDataStructure.UnmarshalSealedUpsertRun(wire, hatDataStructure.SealedUpsertRunOptions{})
		if err != nil || decoded.Len() != recordCount {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(wire)), "wire-bytes/op")
}

func mz005BenchmarkRecords() []hatDataStructure.UpsertRecord[[]byte] {
	records := make([]hatDataStructure.UpsertRecord[[]byte], 4096)
	for index := range records {
		records[index] = hatDataStructure.UpsertRecord[[]byte]{
			Key:   fmt.Sprintf("tenant:region:customer:%08d", index),
			Value: []byte(fmt.Sprintf("status:%08d", index)),
		}
	}
	return records
}

func mz005BenchmarkKeys(records []hatDataStructure.UpsertRecord[[]byte]) []string {
	keys := make([]string, len(records))
	for index, record := range records {
		keys[index] = record.Key
	}
	return keys
}
