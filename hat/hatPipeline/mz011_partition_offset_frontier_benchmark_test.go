package hatPipeline

import (
	"runtime"
	"testing"
)

type mz011ManualPartitionState struct {
	offset    uint64
	watermark uint64
}

var mz011ManualProgressSink bool
var mz011ManualCommonSink uint64
var mz011DenseProgressSink bool
var mz011DenseCommonSink uint64

func BenchmarkMZ011ManualMapAdvance(b *testing.B) {
	const partitionCount = 64
	states := make(map[int32]mz011ManualPartitionState, partitionCount)
	for partition := int32(0); partition < partitionCount; partition++ {
		states[partition] = mz011ManualPartitionState{}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		partition := int32(index & (partitionCount - 1))
		state := states[partition]
		state.offset = uint64(index + 1)
		state.watermark = uint64(index + 1)
		states[partition] = state
		mz011ManualProgressSink = len(states) == partitionCount
	}
}

func BenchmarkMZ011ManualMapCommon(b *testing.B) {
	const partitionCount = 64
	states := make(map[int32]mz011ManualPartitionState, partitionCount)
	for partition := int32(0); partition < partitionCount; partition++ {
		states[partition] = mz011ManualPartitionState{}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		partition := int32(index & (partitionCount - 1))
		state := states[partition]
		state.offset = uint64(index + 1)
		state.watermark = uint64(index + 1)
		states[partition] = state
		common := ^uint64(0)
		for _, candidate := range states {
			if candidate.offset < common {
				common = candidate.offset
			}
		}
		mz011ManualCommonSink = common
	}
}

func BenchmarkMZ011DensePartitionAdvance(b *testing.B) {
	const partitionCount = 64
	frontier, err := NewPartitionOffsetFrontier(mz011BenchmarkPartitions(partitionCount), PartitionOffsetFrontierOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		partition := int32(index & (partitionCount - 1))
		if err := frontier.Advance(partition, uint64(index+1), uint64(index+1)); err != nil {
			b.Fatal(err)
		}
		mz011DenseProgressSink = frontier.Ready()
	}
}

func BenchmarkMZ011DensePartitionCommon(b *testing.B) {
	const partitionCount = 64
	frontier, err := NewPartitionOffsetFrontier(mz011BenchmarkPartitions(partitionCount), PartitionOffsetFrontierOptions{})
	if err != nil {
		b.Fatal(err)
	}
	for partition := int32(0); partition < partitionCount; partition++ {
		if err := frontier.Advance(partition, 0, 0); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		partition := int32(index & (partitionCount - 1))
		if err := frontier.Advance(partition, uint64(index+1), uint64(index+1)); err != nil {
			b.Fatal(err)
		}
		common, _, ready := frontier.Common()
		if !ready {
			b.Fatal("frontier unexpectedly not ready")
		}
		mz011DenseCommonSink = common
	}
}

func BenchmarkMZ011ManualMapBuild(b *testing.B) {
	const partitionCount = 64
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		states := make(map[int32]mz011ManualPartitionState, partitionCount)
		for partition := int32(0); partition < partitionCount; partition++ {
			states[partition] = mz011ManualPartitionState{}
		}
		runtime.KeepAlive(states)
	}
}

func BenchmarkMZ011DenseFrontierBuild(b *testing.B) {
	partitions := mz011BenchmarkPartitions(64)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		frontier, err := NewPartitionOffsetFrontier(partitions, PartitionOffsetFrontierOptions{})
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(frontier)
	}
}

func mz011BenchmarkPartitions(count int) []int32 {
	partitions := make([]int32, count)
	for index := range partitions {
		partitions[index] = int32(index)
	}
	return partitions
}
