package hatSql

import (
	"fmt"
	"sync"
	"testing"
)

func TestAdaptivePlannerConcurrentPredicateFeedback(t *testing.T) {
	planner := NewAdaptivePlanner(AdaptivePlannerOptions{MinSamples: 8, UnderestimateFactor: 2})
	const workers = 32
	const observations = 64
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		key := fmt.Sprintf("CACHE(events).kind=%d", worker)
		group.Add(1)
		go func() {
			defer group.Done()
			for range observations {
				planner.ObserveIndex(key, 1, 3)
				_ = planner.ShouldUseIndex(key)
			}
		}()
	}
	group.Wait()
	for worker := 0; worker < workers; worker++ {
		key := fmt.Sprintf("CACHE(events).kind=%d", worker)
		if planner.ShouldUseIndex(key) {
			t.Fatalf("ShouldUseIndex(%q) = true after persistent underestimation", key)
		}
	}
}
