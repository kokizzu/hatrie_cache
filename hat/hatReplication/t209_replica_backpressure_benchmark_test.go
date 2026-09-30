package hatReplication

import "testing"

var t209BackpressureStateSink ReplicaBackpressureState
var t209BackpressureBoolSink bool

func BenchmarkT209DirectLagGate(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		source := uint64(index + 1024)
		applied := uint64(index)
		lag := uint64(0)
		if source > applied {
			lag = source - applied
		}
		t209BackpressureBoolSink = lag >= 1024
	}
}

func BenchmarkT209ControllerObserve(b *testing.B) {
	controller, err := NewReplicaBackpressureController(ReplicaBackpressureOptions{MaxLag: 1024, ResumeLag: 512})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		state, err := controller.Observe(uint64(index+1024), uint64(index))
		if err != nil {
			b.Fatal(err)
		}
		t209BackpressureStateSink = state
	}
}
