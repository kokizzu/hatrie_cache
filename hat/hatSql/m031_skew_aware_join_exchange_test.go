package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestSkewAwareJoinExchangePromotesHotKeysAndAcknowledgesRebalance(t *testing.T) {
	exchange, err := NewSkewAwareJoinExchange(SkewAwareJoinExchangeOptions{
		Workers:         4,
		HotKeyThreshold: 3,
		BroadcastSide:   IncrementalJoinRight,
	})
	if err != nil {
		t.Fatalf("create exchange: %v", err)
	}

	for index := uint64(0); index < 2; index++ {
		promoted, err := exchange.Observe("hot", 1)
		if err != nil || promoted {
			t.Fatalf("Observe(hot,%d) = promoted=%t err=%v, want false/nil", index+1, promoted, err)
		}
	}
	normal, err := exchange.Route(IncrementalJoinLeft, "hot", "probe-1")
	if err != nil {
		t.Fatalf("normal route: %v", err)
	}
	if normal.Broadcast || normal.Worker < 0 || normal.Worker >= 4 || normal.Hot || normal.RebalanceRequired {
		t.Fatalf("normal route = %+v, want one non-hot worker", normal)
	}
	promoted, err := exchange.Observe("hot", 1)
	if err != nil || !promoted {
		t.Fatalf("Observe(hot,3) = promoted=%t err=%v, want true/nil", promoted, err)
	}

	buildRoute, err := exchange.Route(IncrementalJoinRight, "hot", "build-1")
	if err != nil {
		t.Fatalf("build route: %v", err)
	}
	if !buildRoute.Broadcast || buildRoute.Worker != -1 || !buildRoute.Hot || !buildRoute.RebalanceRequired || buildRoute.Generation == 0 {
		t.Fatalf("build route = %+v, want broadcast hot route", buildRoute)
	}
	workers := buildRoute.Workers(4)
	if !reflect.DeepEqual(workers, []int{0, 1, 2, 3}) {
		t.Fatalf("broadcast workers = %#v, want all workers", workers)
	}
	workers[0] = 99
	if got := buildRoute.Workers(4); !reflect.DeepEqual(got, []int{0, 1, 2, 3}) {
		t.Fatalf("Workers() aliases route storage: %#v", got)
	}

	probeRoutes := make(map[int]struct{})
	for index := 0; index < 32; index++ {
		route, err := exchange.Route(IncrementalJoinLeft, "hot", "probe-"+string(rune('a'+index)))
		if err != nil {
			t.Fatalf("probe route %d: %v", index, err)
		}
		if route.Broadcast || route.Worker < 0 || route.Worker >= 4 || !route.Hot || !route.RebalanceRequired || route.Generation != buildRoute.Generation {
			t.Fatalf("probe route %d = %+v, want one hot worker", index, route)
		}
		probeRoutes[route.Worker] = struct{}{}
	}
	if len(probeRoutes) < 3 {
		t.Fatalf("probe routes used %d workers, want skew spread across at least 3", len(probeRoutes))
	}

	if err := exchange.AcknowledgeRebalance("hot", buildRoute.Generation); err != nil {
		t.Fatalf("acknowledge rebalance: %v", err)
	}
	acknowledged, err := exchange.Route(IncrementalJoinRight, "hot", "build-1")
	if err != nil {
		t.Fatalf("acknowledged route: %v", err)
	}
	if acknowledged.RebalanceRequired {
		t.Fatalf("acknowledged route = %+v, still requires rebalance", acknowledged)
	}

	snapshot := exchange.Snapshot()
	if len(snapshot.HotKeys) != 1 || snapshot.HotKeys[0].Key != "hot" || snapshot.HotKeys[0].Weight != 3 || snapshot.HotKeys[0].RebalanceRequired {
		t.Fatalf("snapshot = %+v, want acknowledged hot key", snapshot)
	}
}

func TestSkewAwareJoinExchangeValidationAndStableColdRoutes(t *testing.T) {
	if _, err := NewSkewAwareJoinExchange(SkewAwareJoinExchangeOptions{}); !errors.Is(err, ErrSkewAwareJoinExchangeWorkersInvalid) {
		t.Fatalf("zero workers error = %v, want workers invalid", err)
	}
	if _, err := NewSkewAwareJoinExchange(SkewAwareJoinExchangeOptions{Workers: 2, HotKeyThreshold: 1, BroadcastSide: IncrementalJoinSide(99)}); !errors.Is(err, ErrSkewAwareJoinExchangeSideInvalid) {
		t.Fatalf("invalid broadcast side error = %v, want side invalid", err)
	}
	exchange, err := NewSkewAwareJoinExchange(SkewAwareJoinExchangeOptions{Workers: 2, HotKeyThreshold: 1})
	if err != nil {
		t.Fatalf("create default exchange: %v", err)
	}
	first, err := exchange.Route(IncrementalJoinLeft, "cold", "source")
	if err != nil {
		t.Fatalf("first cold route: %v", err)
	}
	second, err := exchange.Route(IncrementalJoinRight, "cold", "other")
	if err != nil {
		t.Fatalf("second cold route: %v", err)
	}
	if first != second || first.Broadcast || first.Worker < 0 || first.Worker >= 2 {
		t.Fatalf("cold routes = %+v/%+v, want same single worker", first, second)
	}
	if _, err := exchange.Observe("", 1); !errors.Is(err, ErrSkewAwareJoinExchangeKeyRequired) {
		t.Fatalf("empty observe key error = %v, want key required", err)
	}
	if _, err := exchange.Observe("bad\x00key", 1); !errors.Is(err, ErrSkewAwareJoinExchangeKeyInvalid) {
		t.Fatalf("NUL observe key error = %v, want key invalid", err)
	}
	if _, err := exchange.Observe("cold", 0); !errors.Is(err, ErrSkewAwareJoinExchangeWeightInvalid) {
		t.Fatalf("zero weight error = %v, want weight invalid", err)
	}
	if _, err := exchange.Route(IncrementalJoinSide(99), "cold", "source"); !errors.Is(err, ErrSkewAwareJoinExchangeSideInvalid) {
		t.Fatalf("invalid route side error = %v, want side invalid", err)
	}
	if err := exchange.AcknowledgeRebalance("cold", 1); !errors.Is(err, ErrSkewAwareJoinExchangeRebalanceMissing) {
		t.Fatalf("missing rebalance error = %v, want missing", err)
	}
	if got := exchange.Snapshot(); len(got.HotKeys) != 0 || got.Generation != 0 {
		t.Fatalf("cold snapshot = %+v, want empty", got)
	}
}

func TestSkewAwareJoinExchangeReducesHotKeyWorkerLoad(t *testing.T) {
	events := m031BenchmarkJoinEvents()
	naiveLoads := [4]int{}
	for _, event := range events {
		naiveLoads[int(skewAwareJoinExchangeHash(event.joinKey)%4)]++
	}
	if got := m031MaxWorkerLoad(naiveLoads[:]); got != 85000 {
		t.Fatalf("naive max load = %d, want 85000", got)
	}

	exchange, err := NewSkewAwareJoinExchange(SkewAwareJoinExchangeOptions{
		Workers:         4,
		HotKeyThreshold: 100,
		BroadcastSide:   IncrementalJoinRight,
	})
	if err != nil {
		t.Fatalf("create exchange: %v", err)
	}
	if _, err := exchange.Observe("hot", 100); err != nil {
		t.Fatalf("observe hot key: %v", err)
	}
	loads := [4]int{}
	for _, event := range events {
		route, err := exchange.Route(event.side, event.joinKey, event.sourceKey)
		if err != nil {
			t.Fatalf("route %q: %v", event.sourceKey, err)
		}
		if route.Broadcast {
			for worker := range loads {
				loads[worker]++
			}
		} else {
			loads[route.Worker]++
		}
	}
	if got := m031MaxWorkerLoad(loads[:]); got != 25000 {
		t.Fatalf("skew-aware max load = %d, want 25000", got)
	}
}
