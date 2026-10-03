package hatSql_test

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func mu045Candidate(key string, generation uint64, memory uint64, probe uint64) hatSql.SQLIncrementalJoinCandidate {
	return hatSql.SQLIncrementalJoinCandidate{
		Key:              key,
		LeftSource:       "orders",
		RightSource:      "users",
		LeftField:        "user_id",
		RightField:       "id",
		SourceGeneration: generation,
		BuildCostNanos:   100,
		ProbeCostNanos:   probe,
		ScanCostNanos:    100,
		MaintenanceNanos: 1,
		ExpectedReads:    10,
		ExpectedWrites:   1,
		MemoryBytes:      memory,
	}
}

func TestMU045SelectsCostEffectiveFreshJoinArrangement(t *testing.T) {
	selector := hatSql.NewSQLIncrementalJoinSelector(hatSql.SQLIncrementalJoinSelectorOptions{MinSamples: 1})
	if !selector.ObserveJoin(mu045Candidate("wide", 7, 4096, 4)) {
		t.Fatal("wide candidate was not observed")
	}
	if !selector.ObserveJoin(mu045Candidate("small", 7, 512, 2)) {
		t.Fatal("small candidate was not observed")
	}
	decision := selector.SelectJoin(hatSql.SQLIncrementalJoinRequest{
		LeftSource:       "orders",
		RightSource:      "users",
		LeftField:        "user_id",
		RightField:       "id",
		SourceGeneration: 7,
	})
	if decision.Action != hatSql.SQLIncrementalJoinReuse {
		t.Fatalf("action = %q, want reuse", decision.Action)
	}
	if decision.Candidate.Key != "small" {
		t.Fatalf("candidate = %q, want small", decision.Candidate.Key)
	}
	if decision.Stale {
		t.Fatal("fresh candidate reported stale")
	}
}

func TestMU045RequiresRefreshForOlderJoinArrangement(t *testing.T) {
	selector := hatSql.NewSQLIncrementalJoinSelector(hatSql.SQLIncrementalJoinSelectorOptions{MinSamples: 1})
	selector.ObserveJoin(mu045Candidate("orders-users", 4, 512, 2))
	decision := selector.SelectJoin(hatSql.SQLIncrementalJoinRequest{
		LeftSource:       "orders",
		RightSource:      "users",
		LeftField:        "user_id",
		RightField:       "id",
		SourceGeneration: 7,
	})
	if decision.Action != hatSql.SQLIncrementalJoinRefresh {
		t.Fatalf("action = %q, want refresh", decision.Action)
	}
	if !decision.Stale {
		t.Fatal("older candidate was not marked stale")
	}
	if decision.Candidate.SourceGeneration != 4 {
		t.Fatalf("candidate generation = %d, want 4", decision.Candidate.SourceGeneration)
	}
}

func TestMU045DoesNotUseWrongGenerationOrSignature(t *testing.T) {
	selector := hatSql.NewSQLIncrementalJoinSelector(hatSql.SQLIncrementalJoinSelectorOptions{MinSamples: 1})
	selector.ObserveJoin(mu045Candidate("orders-users", 8, 512, 2))
	wrong := selector.SelectJoin(hatSql.SQLIncrementalJoinRequest{
		LeftSource:       "orders",
		RightSource:      "accounts",
		LeftField:        "user_id",
		RightField:       "id",
		SourceGeneration: 8,
	})
	if wrong.Action != hatSql.SQLIncrementalJoinCreate {
		t.Fatalf("wrong signature action = %q, want create", wrong.Action)
	}
	future := selector.SelectJoin(hatSql.SQLIncrementalJoinRequest{
		LeftSource:       "orders",
		RightSource:      "users",
		LeftField:        "user_id",
		RightField:       "id",
		SourceGeneration: 9,
	})
	if future.Action != hatSql.SQLIncrementalJoinRefresh {
		t.Fatalf("future generation action = %q, want refresh", future.Action)
	}
}

func TestMU045BoundsCandidatesAndReturnsIndependentSnapshot(t *testing.T) {
	selector := hatSql.NewSQLIncrementalJoinSelector(hatSql.SQLIncrementalJoinSelectorOptions{Capacity: 2, MinSamples: 1})
	first := mu045Candidate("first", 1, 512, 2)
	second := mu045Candidate("second", 1, 512, 2)
	third := mu045Candidate("third", 1, 512, 2)
	third.LeftSource = "payments"
	if !selector.ObserveJoin(first) || !selector.ObserveJoin(second) {
		t.Fatal("bounded selector rejected an available slot")
	}
	if selector.ObserveJoin(third) {
		t.Fatal("bounded selector admitted a third candidate")
	}
	snapshot := selector.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("snapshot length = %d, want 2", len(snapshot))
	}
	snapshot[0].Candidate.Key = "mutated"
	if got := selector.Snapshot()[0].Candidate.Key; got == "mutated" {
		t.Fatal("snapshot aliases selector state")
	}
	if got := selector.Len(); got != 2 {
		t.Fatalf("selector length = %d, want 2", got)
	}
}

func TestMU045NormalizesJoinIdentity(t *testing.T) {
	selector := hatSql.NewSQLIncrementalJoinSelector(hatSql.SQLIncrementalJoinSelectorOptions{MinSamples: 1})
	candidate := mu045Candidate("normalized", 3, 512, 2)
	candidate.LeftSource = " ORDERS "
	candidate.RightSource = "USERS"
	candidate.LeftField = "o.USER_ID"
	candidate.RightField = " u.id "
	if !selector.ObserveJoin(candidate) {
		t.Fatal("normalized candidate was not observed")
	}
	decision := selector.SelectJoin(hatSql.SQLIncrementalJoinRequest{
		LeftSource:       "orders",
		RightSource:      "users",
		LeftField:        "user_id",
		RightField:       "id",
		SourceGeneration: 3,
	})
	if decision.Action != hatSql.SQLIncrementalJoinReuse {
		t.Fatalf("action = %q, want reuse", decision.Action)
	}
	want := "normalized"
	if !reflect.DeepEqual(decision.Candidate.Key, want) {
		t.Fatalf("candidate = %q, want %q", decision.Candidate.Key, want)
	}
}

func TestMU045RequiresConfiguredSampleCountAndKeepsGenerationMonotonic(t *testing.T) {
	selector := hatSql.NewSQLIncrementalJoinSelector(hatSql.SQLIncrementalJoinSelectorOptions{})
	candidate := mu045Candidate("sampled", 4, 512, 2)
	if !selector.ObserveJoin(candidate) {
		t.Fatal("first sample was not observed")
	}
	request := hatSql.SQLIncrementalJoinRequest{
		LeftSource:       "orders",
		RightSource:      "users",
		LeftField:        "user_id",
		RightField:       "id",
		SourceGeneration: 4,
	}
	if decision := selector.SelectJoin(request); decision.Action != hatSql.SQLIncrementalJoinCreate {
		t.Fatalf("one-sample action = %q, want create", decision.Action)
	}
	newer := candidate
	newer.SourceGeneration = 6
	if !selector.ObserveJoin(newer) {
		t.Fatal("second sample was not observed")
	}
	older := candidate
	older.SourceGeneration = 2
	if !selector.ObserveJoin(older) {
		t.Fatal("out-of-order sample was not observed")
	}
	request.SourceGeneration = 6
	decision := selector.SelectJoin(request)
	if decision.Action != hatSql.SQLIncrementalJoinReuse {
		t.Fatalf("two-sample action = %q, want reuse", decision.Action)
	}
	if decision.Candidate.SourceGeneration != 6 {
		t.Fatalf("generation = %d, want 6", decision.Candidate.SourceGeneration)
	}
	if decision.Samples != 3 {
		t.Fatalf("samples = %d, want 3", decision.Samples)
	}
}

func TestMU045RejectsUnboundedIdentity(t *testing.T) {
	selector := hatSql.NewSQLIncrementalJoinSelector(hatSql.SQLIncrementalJoinSelectorOptions{MinSamples: 1})
	candidate := mu045Candidate("valid", 1, 512, 2)
	candidate.LeftSource = strings.Repeat("x", 129)
	if selector.ObserveJoin(candidate) {
		t.Fatal("overlong source was accepted")
	}
	candidate = mu045Candidate("valid", 1, 512, 2)
	candidate.LeftField = strings.Repeat("x", 129)
	if selector.ObserveJoin(candidate) {
		t.Fatal("overlong field was accepted")
	}
}

var mu045BenchmarkSink string

func mu045BenchmarkSelector() (*hatSql.SQLIncrementalJoinSelector, []hatSql.SQLIncrementalJoinSnapshot, hatSql.SQLIncrementalJoinRequest) {
	selector := hatSql.NewSQLIncrementalJoinSelector(hatSql.SQLIncrementalJoinSelectorOptions{Capacity: 128, MinSamples: 1})
	for index := 0; index < 128; index++ {
		candidate := mu045Candidate("candidate-"+strconv.Itoa(index), 1, uint64(512+index), 2)
		candidate.LeftSource = "left-" + strconv.Itoa(index%64)
		candidate.RightSource = "right"
		candidate.LeftField = "key"
		candidate.RightField = "id"
		if !selector.ObserveJoin(candidate) {
			panic("benchmark candidate was rejected")
		}
	}
	return selector, selector.Snapshot(), hatSql.SQLIncrementalJoinRequest{
		LeftSource:       "left-63",
		RightSource:      "right",
		LeftField:        "key",
		RightField:       "id",
		SourceGeneration: 1,
	}
}

func BenchmarkMU045IndexedSelection(b *testing.B) {
	selector, _, request := mu045BenchmarkSelector()
	b.ReportAllocs()
	for range b.N {
		mu045BenchmarkSink = selector.SelectJoin(request).Candidate.Key
	}
}

func BenchmarkMU045LinearSelection(b *testing.B) {
	_, snapshot, request := mu045BenchmarkSelector()
	b.ReportAllocs()
	for range b.N {
		mu045BenchmarkSink = mu045LinearSelection(snapshot, request)
	}
}

func mu045LinearSelection(snapshot []hatSql.SQLIncrementalJoinSnapshot, request hatSql.SQLIncrementalJoinRequest) string {
	model := hatSql.NewSQLArrangementCostModel(hatSql.SQLArrangementCostModelOptions{})
	bestKey := ""
	bestStale := true
	bestScore := hatSql.SQLArrangementCostScore{}
	found := false
	for _, item := range snapshot {
		candidate := item.Candidate
		if candidate.LeftSource != request.LeftSource || candidate.RightSource != request.RightSource || candidate.LeftField != request.LeftField || candidate.RightField != request.RightField || candidate.SourceGeneration > request.SourceGeneration || item.Samples < 1 {
			continue
		}
		score := model.Evaluate(hatSql.SQLArrangementCostCandidate{
			Key:              candidate.Key,
			Field:            candidate.LeftField + "=" + candidate.RightField,
			Kind:             "join",
			BuildCostNanos:   candidate.BuildCostNanos,
			ProbeCostNanos:   candidate.ProbeCostNanos,
			ScanCostNanos:    candidate.ScanCostNanos,
			MaintenanceNanos: candidate.MaintenanceNanos,
			ExpectedReads:    candidate.ExpectedReads,
			ExpectedWrites:   candidate.ExpectedWrites,
			MemoryBytes:      candidate.MemoryBytes,
		})
		if !score.Reusable {
			continue
		}
		stale := candidate.SourceGeneration < request.SourceGeneration
		if !found || (bestStale && !stale) || (bestStale == stale && score.NetBenefitNanos > bestScore.NetBenefitNanos) {
			bestKey, bestStale, bestScore, found = candidate.Key, stale, score, true
		}
	}
	return bestKey
}
