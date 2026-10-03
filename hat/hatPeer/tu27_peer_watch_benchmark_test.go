package hatPeer

import (
	"encoding/json"
	"testing"
)

type tu27ConfigWatchJSONBenchmarkEvent struct {
	Revision uint64 `json:"revision"`
	Path     string `json:"path"`
	Value    []byte `json:"value"`
}

var tu27ConfigWatchBenchmarkEvent = CompactPeerConfigWatchEvent{
	Revision: 42,
	Path:     "region/asia-southeast-1/service/cache/feature-flags",
	Value:    []byte("enabled=true;rollout=75;owner=platform;expires=2026-12-31"),
}

func BenchmarkTU27ConfigWatchJSONEncode(b *testing.B) {
	event := tu27ConfigWatchJSONBenchmarkEvent{
		Revision: tu27ConfigWatchBenchmarkEvent.Revision,
		Path:     tu27ConfigWatchBenchmarkEvent.Path,
		Value:    tu27ConfigWatchBenchmarkEvent.Value,
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(encoded)), "wire-B")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := json.Marshal(event); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU27ConfigWatchJSONDecode(b *testing.B) {
	encoded, err := json.Marshal(tu27ConfigWatchJSONBenchmarkEvent{
		Revision: tu27ConfigWatchBenchmarkEvent.Revision,
		Path:     tu27ConfigWatchBenchmarkEvent.Path,
		Value:    tu27ConfigWatchBenchmarkEvent.Value,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(encoded)), "wire-B")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var event tu27ConfigWatchJSONBenchmarkEvent
		if err := json.Unmarshal(encoded, &event); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU27ConfigWatchBinaryEncode(b *testing.B) {
	events := []CompactPeerConfigWatchEvent{tu27ConfigWatchBenchmarkEvent}
	encoded, err := marshalCompactPeerConfigWatchEvents(tu27ConfigWatchBenchmarkEvent.Revision, events)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(encoded)), "wire-B")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := marshalCompactPeerConfigWatchEvents(tu27ConfigWatchBenchmarkEvent.Revision, events); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU27ConfigWatchBinaryDecode(b *testing.B) {
	encoded, err := marshalCompactPeerConfigWatchEvents(tu27ConfigWatchBenchmarkEvent.Revision, []CompactPeerConfigWatchEvent{tu27ConfigWatchBenchmarkEvent})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(encoded)), "wire-B")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, _, err := unmarshalCompactPeerConfigWatchEvents(encoded, CompactPeerConfigWatchOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
