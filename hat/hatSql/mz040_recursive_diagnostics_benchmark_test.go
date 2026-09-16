package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkMZ040RecursiveReachabilityApply(b *testing.B) {
	seed := mz040BenchmarkSeed()
	mutation := []RecursiveReachabilityMutation{{Kind: RecursiveReachabilityInsert, Key: "tail:new", From: "node:255", To: "leaf:new"}}
	b.Run("legacy_apply", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			reachability := mz040BenchmarkReachability(seed)
			b.StartTimer()
			updates, err := reachability.Apply(mutation)
			b.StopTimer()
			if err != nil || len(updates) == 0 {
				b.Fatalf("Apply() updates/error = %d/%v", len(updates), err)
			}
		}
	})
	b.Run("bounded_apply", func(b *testing.B) {
		b.ReportAllocs()
		options := RecursiveReachabilityApplyOptions{
			MaxAffectedSources: 256,
			MaxEmittedUpdates:  256,
		}
		for range b.N {
			reachability := mz040BenchmarkReachability(seed)
			b.StartTimer()
			updates, _, err := reachability.ApplyWithOptions(mutation, options)
			b.StopTimer()
			if err != nil || len(updates) == 0 {
				b.Fatalf("ApplyWithOptions() updates/error = %d/%v", len(updates), err)
			}
		}
	})
	b.Run("fully_bounded_apply", func(b *testing.B) {
		b.ReportAllocs()
		options := RecursiveReachabilityApplyOptions{
			MaxAffectedSources: 256,
			MaxIterations:      256,
			MaxTraversalSteps:  100000,
			MaxEmittedUpdates:  256,
		}
		for range b.N {
			reachability := mz040BenchmarkReachability(seed)
			b.StartTimer()
			updates, _, err := reachability.ApplyWithOptions(mutation, options)
			b.StopTimer()
			if err != nil || len(updates) == 0 {
				b.Fatalf("ApplyWithOptions() updates/error = %d/%v", len(updates), err)
			}
		}
	})
}

func mz040BenchmarkSeed() []RecursiveReachabilityMutation {
	seed := make([]RecursiveReachabilityMutation, 0, 255)
	for index := 0; index < 255; index++ {
		seed = append(seed, RecursiveReachabilityMutation{
			Kind: RecursiveReachabilityInsert,
			Key:  "edge:" + strconv.Itoa(index),
			From: "node:" + strconv.Itoa(index),
			To:   "node:" + strconv.Itoa(index+1),
		})
	}
	return seed
}

func mz040BenchmarkReachability(seed []RecursiveReachabilityMutation) *IncrementalRecursiveReachability {
	reachability := NewMutableIncrementalRecursiveReachability()
	if _, err := reachability.Apply(seed); err != nil {
		panic(err)
	}
	return reachability
}
