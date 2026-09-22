package hatReplication

import (
	"encoding/json"
	"strconv"
	"testing"
)

var (
	changefeedExactlyOnceBenchmarkSnapshot = ChangefeedExactlyOnceSnapshot{
		Source:               "orders/sg",
		HasCommittedOffset:   true,
		CommittedOffset:      1 << 32,
		CommittedFrontier:    987654321,
		LastCommittedBatchID: "batch-0000000000000042",
		Pending: &ChangefeedExactlyOnceBatch{
			ID:          "batch-0000000000000043",
			StartOffset: 1<<32 + 1,
			EndOffset:   1<<32 + 100,
		},
	}
	changefeedExactlyOnceBenchmarkBinary []byte
	changefeedExactlyOnceBenchmarkJSON   []byte
)

func init() {
	var err error
	changefeedExactlyOnceBenchmarkBinary, err = changefeedExactlyOnceBenchmarkSnapshot.MarshalBinary()
	if err != nil {
		panic(err)
	}
	changefeedExactlyOnceBenchmarkJSON, err = json.Marshal(changefeedExactlyOnceBenchmarkSnapshot)
	if err != nil {
		panic(err)
	}
}

func TestChangefeedExactlyOnceBinaryIsSmallerThanJSON(t *testing.T) {
	if len(changefeedExactlyOnceBenchmarkBinary) >= len(changefeedExactlyOnceBenchmarkJSON) {
		t.Fatalf("binary snapshot should be smaller: binary=%d json=%d", len(changefeedExactlyOnceBenchmarkBinary), len(changefeedExactlyOnceBenchmarkJSON))
	}
	t.Logf("binary=%d bytes json=%d bytes reduction=%.2fx", len(changefeedExactlyOnceBenchmarkBinary), len(changefeedExactlyOnceBenchmarkJSON), float64(len(changefeedExactlyOnceBenchmarkJSON))/float64(len(changefeedExactlyOnceBenchmarkBinary)))
}

func BenchmarkChangefeedExactlyOnceBinaryMarshal(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := changefeedExactlyOnceBenchmarkSnapshot.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		changefeedExactlyOnceBenchmarkBinary = encoded
	}
}

func BenchmarkChangefeedExactlyOnceJSONMarshal(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := json.Marshal(changefeedExactlyOnceBenchmarkSnapshot)
		if err != nil {
			b.Fatal(err)
		}
		changefeedExactlyOnceBenchmarkJSON = encoded
	}
}

func BenchmarkChangefeedExactlyOnceBinaryUnmarshal(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		decoded, err := UnmarshalChangefeedExactlyOnceSnapshot(changefeedExactlyOnceBenchmarkBinary)
		if err != nil {
			b.Fatal(err)
		}
		changefeedExactlyOnceBenchmarkSnapshot = decoded
	}
}

func BenchmarkChangefeedExactlyOnceJSONUnmarshal(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var decoded ChangefeedExactlyOnceSnapshot
		if err := json.Unmarshal(changefeedExactlyOnceBenchmarkJSON, &decoded); err != nil {
			b.Fatal(err)
		}
		changefeedExactlyOnceBenchmarkSnapshot = decoded
	}
}

func BenchmarkChangefeedExactlyOnceCommitAndRestartOffset(b *testing.B) {
	consumer, err := NewChangefeedExactlyOnceConsumer("benchmark")
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]string, b.N)
	for i := range ids {
		ids[i] = "batch-" + strconv.FormatInt(int64(i), 10)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i, id := range ids {
		offset := uint64(i)
		if _, err := consumer.BeginBatch(id, offset, offset); err != nil {
			b.Fatal(err)
		}
		if _, err := consumer.CommitBatch(id, offset); err != nil {
			b.Fatal(err)
		}
		if _, err := consumer.RestartOffset(); err != nil {
			b.Fatal(err)
		}
	}
}
