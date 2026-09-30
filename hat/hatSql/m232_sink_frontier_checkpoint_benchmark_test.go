package hatSql

import "testing"

func BenchmarkM232ExistingSinkProgressAcknowledge(b *testing.B) {
	tracker := NewSQLSinkProgressTracker()
	progress := SQLSinkProgress{Sink: "orders-sink", Partition: "region-a"}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		progress.Frontier = uint64(iteration + 1)
		if _, err := tracker.Acknowledge(progress); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM232FrontierCheckpointEmitAndAcknowledge(b *testing.B) {
	coordinator := NewSQLSinkFrontierCheckpointCoordinator()
	message := SQLSinkFrontierMessage{
		Sink:           "orders-sink",
		Partition:      "region-a",
		SubscriptionID: 42,
		Revision:       1,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		message.Frontier = uint64(iteration + 1)
		if _, err := coordinator.Emit(message); err != nil {
			b.Fatal(err)
		}
		if _, err := coordinator.Acknowledge(message); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM232FrontierCheckpointSnapshot(b *testing.B) {
	coordinator := NewSQLSinkFrontierCheckpointCoordinator()
	for partition := 0; partition < 64; partition++ {
		message := SQLSinkFrontierMessage{
			Sink:           "orders-sink",
			Partition:      "region-" + string(rune('a'+partition)),
			SubscriptionID: 42,
			Revision:       1,
			Frontier:       uint64(partition + 1),
		}
		if _, err := coordinator.Emit(message); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_ = coordinator.Snapshot()
	}
}
