package hatReplication

import "testing"

type tu39BaselineEvent struct {
	key   []byte
	after []byte
}

var tu39BaselineSink []tu39BaselineEvent

func BenchmarkTU39BaselineDirectEventAppend(b *testing.B) {
	event := tu39BaselineEvent{key: []byte("order-1"), after: []byte(`{"status":"paid"}`)}
	buffer := make([]tu39BaselineEvent, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		buffer[index%len(buffer)] = event
	}
	b.StopTimer()
	tu39BaselineSink = buffer
}
