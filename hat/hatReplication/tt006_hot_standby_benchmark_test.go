package hatReplication

import "testing"

var tt006HotStandbyBenchmarkState HotStandbyState
var tt006HotStandbyBenchmarkOK bool

func BenchmarkTT006BaselineReplayBookkeeping(b *testing.B) {
	term, fence := uint64(7), uint64(99)
	advertised, applied := uint64(100), uint64(100)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		advertised += 2
		if term != 7 || fence != 99 || advertised-applied > 16 {
			b.Fatal("baseline validation failed")
		}
		first, last := applied+1, advertised
		if first != applied+1 || last < first || last > advertised {
			b.Fatal("baseline sequence validation failed")
		}
		applied = last
	}
	tt006HotStandbyBenchmarkOK = applied == advertised
}

func BenchmarkTT006ValidatedReplayBookkeeping(b *testing.B) {
	coordinator, err := NewHotStandbyCoordinator(HotStandbyOptions{MaxLag: 16})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := coordinator.Start(HotStandbyPlan{
		StandbyID:         "standby-a",
		PrimaryID:         "primary-a",
		StorageGeneration: 4,
		SnapshotSequence:  100,
		SourceTerm:        7,
		FencingToken:      99,
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		state := coordinator.Snapshot()
		head := state.AppliedSequence + 2
		if _, err := coordinator.ObserveHead(7, head, 99); err != nil {
			b.Fatal(err)
		}
		if state, err = coordinator.ApplyWAL(7, head-1, head, 99); err != nil {
			b.Fatal(err)
		}
		tt006HotStandbyBenchmarkState = state
	}
	tt006HotStandbyBenchmarkOK = tt006HotStandbyBenchmarkState.AppliedSequence == tt006HotStandbyBenchmarkState.AdvertisedSequence
}
