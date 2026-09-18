package hatPipeline

import "testing"

var mz010CutoverStatusSink SnapshotCutoverStatus
var mz010CutoverProgressSink SnapshotCutoverProgress
var mz010ManualReadySink bool

func BenchmarkMZ010CoordinatorAcknowledge(b *testing.B) {
	coordinator, err := NewSnapshotCutoverCoordinator(SnapshotCutoverOptions{MaxCutovers: 1, MaxSources: 16})
	if err != nil {
		b.Fatal(err)
	}
	sources := make([]SnapshotCutoverSource, 16)
	for index := range sources {
		sources[index] = SnapshotCutoverSource{ID: "source-" + benchmarkMZ010Integer(index), Generation: 1}
	}
	if _, err := coordinator.Prepare(SnapshotCutoverSpec{ID: "cutover", Timestamp: 100, Sources: sources}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		status, err := coordinator.Acknowledge("cutover", SnapshotCutoverAcknowledgement{
			SourceID:   sources[index%len(sources)].ID,
			Generation: 1,
			Lower:      100,
			Upper:      uint64(index + 100),
		})
		if err != nil {
			b.Fatal(err)
		}
		mz010CutoverStatusSink = status
	}
}

func BenchmarkMZ010CoordinatorAcknowledgeProgress(b *testing.B) {
	coordinator, err := NewSnapshotCutoverCoordinator(SnapshotCutoverOptions{MaxCutovers: 1, MaxSources: 16})
	if err != nil {
		b.Fatal(err)
	}
	sources := make([]SnapshotCutoverSource, 16)
	for index := range sources {
		sources[index] = SnapshotCutoverSource{ID: "source-" + benchmarkMZ010Integer(index), Generation: 1}
	}
	if _, err := coordinator.Prepare(SnapshotCutoverSpec{ID: "cutover", Timestamp: 100, Sources: sources}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		progress, err := coordinator.AcknowledgeProgress("cutover", SnapshotCutoverAcknowledgement{
			SourceID:   sources[index%len(sources)].ID,
			Generation: 1,
			Lower:      100,
			Upper:      uint64(index + 100),
		})
		if err != nil {
			b.Fatal(err)
		}
		mz010CutoverProgressSink = progress
	}
}

func BenchmarkMZ010ManualSourceCoverage(b *testing.B) {
	acknowledgements := make([]SnapshotCutoverAcknowledgement, 16)
	for index := range acknowledgements {
		acknowledgements[index] = SnapshotCutoverAcknowledgement{SourceID: "source-" + benchmarkMZ010Integer(index), Generation: 1, Lower: 100, Upper: 100}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		sourceIndex := index % len(acknowledgements)
		acknowledgements[sourceIndex].Upper = uint64(index + 100)
		ready := true
		for _, acknowledgement := range acknowledgements {
			if acknowledgement.Generation != 1 || acknowledgement.Lower < 100 {
				ready = false
				break
			}
		}
		mz010ManualReadySink = ready
	}
}

func benchmarkMZ010Integer(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}
