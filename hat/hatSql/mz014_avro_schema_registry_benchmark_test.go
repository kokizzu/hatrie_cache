package hatSql

import (
	"encoding/binary"
	"errors"
	"testing"
)

var mz014BenchmarkSink uint32

func mz014BenchmarkPayloads() [][]byte {
	payloads := make([][]byte, 10000)
	for index := range payloads {
		payloads[index] = []byte{0, 0, 0, 0, 7, byte(index), byte(index >> 8)}
	}
	return payloads
}

func mz014BenchmarkFetch(schemaID uint32) ([]byte, error) {
	schema := []byte(`{"type":"record","name":"event","fields":[{"name":"id","type":"long"}]}`)
	if schemaID != 7 {
		return nil, errors.New("benchmark schema not found")
	}
	return append([]byte(nil), schema...), nil
}

// BenchmarkMZ014AvroSchemaRegistryBaseline measures a naïve source decoder
// that fetches and allocates the same schema for every record.
func BenchmarkMZ014AvroSchemaRegistryBaseline(b *testing.B) {
	payloads := mz014BenchmarkPayloads()
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		var sink uint32
		for _, payload := range payloads {
			schema, err := mz014BenchmarkFetch(binary.BigEndian.Uint32(payload[1:5]))
			if err != nil {
				b.Fatal(err)
			}
			sink += uint32(len(schema)) + binary.BigEndian.Uint32(payload[1:5])
		}
		mz014BenchmarkSink = sink
	}
}

// BenchmarkMZ014AvroSchemaRegistryCached measures the hot cache path used by
// a source decoder after the schema ID has been resolved once.
func BenchmarkMZ014AvroSchemaRegistryCached(b *testing.B) {
	payloads := mz014BenchmarkPayloads()
	registry, err := NewAvroSchemaRegistry(AvroSchemaRegistryOptions{Fetch: mz014BenchmarkFetch})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := registry.Borrow(7); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		var sink uint32
		for _, payload := range payloads {
			schema, err := registry.Borrow(binary.BigEndian.Uint32(payload[1:5]))
			if err != nil {
				b.Fatal(err)
			}
			sink += uint32(len(schema)) + binary.BigEndian.Uint32(payload[1:5])
		}
		mz014BenchmarkSink = sink
	}
}
