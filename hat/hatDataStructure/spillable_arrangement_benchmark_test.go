package hatDataStructure_test

import (
	"bytes"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func BenchmarkSpillableArrangementInMemoryBaseline(b *testing.B) {
	values := spillableArrangementBenchmarkValues()
	keys := spillableArrangementBenchmarkKeys()
	resident := make(map[string][]byte, len(values))
	for key, value := range values {
		resident[key] = value
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum byte
	for operation := 0; operation < b.N; operation++ {
		value := append([]byte(nil), resident[keys[operation%len(keys)]]...)
		checksum ^= value[operation%len(value)]
	}
	b.StopTimer()
	if checksum == 0xff {
		b.Fatal("unexpected checksum")
	}
}

func BenchmarkSpillableArrangementColdGet(b *testing.B) {
	arrangement, err := hatDataStructure.NewSpillableArrangement(hatDataStructure.SpillableArrangementOptions{
		MemoryLimitBytes: 1,
		MaxDiskBytes:     64 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer arrangement.Close()
	values := spillableArrangementBenchmarkValues()
	keys := spillableArrangementBenchmarkKeys()
	for key, value := range values {
		if err := arrangement.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
	if stats := arrangement.Stats(); stats.ColdEntries != len(values) {
		b.Fatalf("cold entries = %d, want %d", stats.ColdEntries, len(values))
	}
	b.ReportAllocs()
	b.ResetTimer()
	var checksum byte
	for operation := 0; operation < b.N; operation++ {
		value, found, err := arrangement.Get(keys[operation%len(keys)])
		if err != nil || !found {
			b.Fatal(err)
		}
		checksum ^= value[operation%len(value)]
	}
	b.StopTimer()
	if checksum == 0xff {
		b.Fatal("unexpected checksum")
	}
}

func spillableArrangementBenchmarkValues() map[string][]byte {
	values := make(map[string][]byte, 4096)
	for index := 0; index < 4096; index++ {
		values[fmt.Sprintf("key-%04d", index)] = bytes.Repeat([]byte{byte(index)}, 256)
	}
	return values
}

func spillableArrangementBenchmarkKeys() []string {
	keys := make([]string, 4096)
	for index := range keys {
		keys[index] = fmt.Sprintf("key-%04d", index)
	}
	return keys
}

func newRound60SpillableCompactionArrangement(b *testing.B) *hatDataStructure.SpillableArrangement {
	b.Helper()
	arrangement, err := hatDataStructure.NewSpillableArrangement(hatDataStructure.SpillableArrangementOptions{
		MemoryLimitBytes: 1,
		MaxDiskBytes:     128 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	value := bytes.Repeat([]byte{'v'}, 128)
	for index := 0; index < 512; index++ {
		if err := arrangement.Set(fmt.Sprintf("key-%04d", index), value); err != nil {
			_ = arrangement.Close()
			b.Fatal(err)
		}
	}
	if err := arrangement.Flush(); err != nil {
		_ = arrangement.Close()
		b.Fatal(err)
	}
	if stats := arrangement.Stats(); stats.ColdEntries != 512 {
		_ = arrangement.Close()
		b.Fatalf("cold entries = %d, want 512", stats.ColdEntries)
	}
	b.Cleanup(func() { _ = arrangement.Close() })
	return arrangement
}

func BenchmarkMZ028SpillableCompactFreshSegment(b *testing.B) {
	arrangement := newRound60SpillableCompactionArrangement(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := arrangement.Compact(); err != nil {
			b.Fatal(err)
		}
	}
}
