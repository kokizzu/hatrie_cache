package hatriecache

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

func BenchmarkCommandWireJSON(b *testing.B) {
	benchmarkCommandWireFormat(b, CommandWireFormatJSON)
}

func BenchmarkCommandWireProtobuf(b *testing.B) {
	benchmarkCommandWireFormat(b, CommandWireFormatProtobuf)
}

func benchmarkCommandWireFormat(b *testing.B, format CommandWireFormat) {
	payload := benchmarkCommandWirePayload()
	wireBytes := benchmarkCommandWireBytes(b, payload, format)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(wireBytes), "wire_B/op")
	for i := 0; i < b.N; i++ {
		body, _, _, err := commandRequestBody(payload, format, estimatedReplicationRequestBytes(payload), minCompressedReplicationRequestBytes)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, body); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCommandWireBytes(b *testing.B, payload CacheCommandRequest, format CommandWireFormat) int {
	b.Helper()
	body, _, _, err := commandRequestBody(payload, format, estimatedReplicationRequestBytes(payload), minCompressedReplicationRequestBytes)
	if err != nil {
		b.Fatal(err)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		b.Fatal(err)
	}
	return len(data)
}

func benchmarkCommandWirePayload() CacheCommandRequest {
	entry := `{"key":"session:1","type":"string","string":"` + strings.Repeat("active-user-", 256) + `","map":null,"slice":null,"set":null,"priority_queue":null}`
	return CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "session:1",
		Value:   entry,
	}
}

func BenchmarkSnapshotFormatJSON(b *testing.B) {
	benchmarkSnapshotFormat(b, SnapshotFormatJSON)
}

func BenchmarkSnapshotFormatGzipJSON(b *testing.B) {
	benchmarkSnapshotFormat(b, SnapshotFormatGzipJSON)
}

func benchmarkSnapshotFormat(b *testing.B, format SnapshotFormat) {
	trie := benchmarkSnapshotTrie()
	defer trie.Destroy()
	diskBytes := benchmarkSnapshotBytes(b, trie, format)
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(diskBytes), "disk_B/op")
	for i := 0; i < b.N; i++ {
		if err := trie.writeSnapshot(io.Discard, 42, format); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSnapshotBytes(b *testing.B, trie *HatTrie, format SnapshotFormat) int {
	b.Helper()
	var builder strings.Builder
	if err := trie.writeSnapshot(&builder, 42, format); err != nil {
		b.Fatal(err)
	}
	return builder.Len()
}

func benchmarkSnapshotTrie() *HatTrie {
	trie := CreateHatTrie()
	payload := strings.Repeat("payload-", 64)
	for idx := 0; idx < 512; idx++ {
		trie.UpsertString(fmt.Sprintf("session:%04d", idx), payload)
	}
	return trie
}
