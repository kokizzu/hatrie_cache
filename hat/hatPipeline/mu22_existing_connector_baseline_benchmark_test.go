package hatPipeline

import "testing"

func BenchmarkMU22BaselineConnectorCheckpointEncode(b *testing.B) {
	checkpoint := ConnectorCheckpoint{
		ConnectorID: "orders",
		Sequence:    42,
		Generation: 7,
		Offset:      []byte("offset-42"),
		Frontier:    []byte("frontier-42"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := EncodeConnectorCheckpoint(checkpoint); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU22BaselineConnectorCheckpointDecode(b *testing.B) {
	checkpoint := ConnectorCheckpoint{
		ConnectorID: "orders",
		Sequence:    42,
		Generation: 7,
		Offset:      []byte("offset-42"),
		Frontier:    []byte("frontier-42"),
	}
	payload, err := EncodeConnectorCheckpoint(checkpoint)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := DecodeConnectorCheckpoint(payload); err != nil {
			b.Fatal(err)
		}
	}
}
