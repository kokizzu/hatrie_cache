package hatReplication

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func TestGlobalTimestampOracleReservesOrderedRanges(t *testing.T) {
	oracle, err := NewGlobalTimestampOracle(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := GlobalTimestampRequest{
		Term: 1, NodeID: " node-a ", NodeEpoch: 1, Sequence: 1, Count: 3,
	}
	first, err := oracle.Reserve(firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	if first.NodeID != "node-a" || first.Start != 1 || first.End != 3 {
		t.Fatalf("first grant = %#v, want node-a [1,3]", first)
	}
	lease, err := NewGlobalTimestampLease(first)
	if err != nil {
		t.Fatal(err)
	}
	for want := int64(1); want <= 3; want++ {
		if got, ok := lease.Next(); !ok || got != want {
			t.Fatalf("lease.Next() = %d, %v, want %d, true", got, ok, want)
		}
	}
	if got, ok := lease.Next(); ok || got != 0 {
		t.Fatalf("exhausted lease.Next() = %d, %v, want 0, false", got, ok)
	}
	if err := lease.Reset(GlobalTimestampGrant{
		Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 2, Start: 100, End: 101, Count: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if got, ok := lease.Next(); !ok || got != 100 || lease.Remaining() != 1 {
		t.Fatalf("reset lease.Next() = %d, %v, remaining %d, want 100, true, 1", got, ok, lease.Remaining())
	}

	second, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 1, NodeID: "node-b", NodeEpoch: 4, Sequence: 1, Observed: 1, Count: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Start != 4 || second.End != 5 {
		t.Fatalf("second grant = %#v, want [4,5]", second)
	}
	third, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 1, NodeID: "node-c", NodeEpoch: 1, Sequence: 1, Observed: 10, Count: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if third.Start != 11 || third.End != 11 || oracle.Current() != 11 {
		t.Fatalf("observed-jump grant = %#v, current = %d, want [11,11], 11", third, oracle.Current())
	}
}

func TestGlobalTimestampOracleRetriesAndFencesNodeSequences(t *testing.T) {
	oracle, err := NewGlobalTimestampOracle(7, 0)
	if err != nil {
		t.Fatal(err)
	}
	request := GlobalTimestampRequest{
		Term: 7, NodeID: "node-a", NodeEpoch: 2, Sequence: 1, Count: 2,
	}
	first, err := oracle.Reserve(request)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := oracle.Reserve(request)
	if err != nil {
		t.Fatalf("identical retry error = %v", err)
	}
	if retry != first {
		t.Fatalf("identical retry = %#v, want %#v", retry, first)
	}
	conflict := request
	conflict.Count++
	if _, err := oracle.Reserve(conflict); !errors.Is(err, ErrGlobalTimestampOracleRequestConflict) {
		t.Fatalf("conflicting retry error = %v, want request conflict", err)
	}
	if _, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 7, NodeID: "node-a", NodeEpoch: 2, Sequence: 3, Count: 1,
	}); !errors.Is(err, ErrGlobalTimestampOracleSequenceGap) {
		t.Fatalf("sequence gap error = %v, want sequence gap", err)
	}
	if _, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 7, NodeID: "node-a", NodeEpoch: 2, Sequence: 0, Count: 1,
	}); !errors.Is(err, ErrGlobalTimestampOracleInvalid) {
		t.Fatalf("zero sequence error = %v, want invalid", err)
	}
	second, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 7, NodeID: "node-a", NodeEpoch: 2, Sequence: 2, Count: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Start != 3 {
		t.Fatalf("second node grant = %#v, want start 3", second)
	}
	if _, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 7, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1,
	}); !errors.Is(err, ErrGlobalTimestampOracleNodeEpochStale) {
		t.Fatalf("stale node epoch error = %v, want stale epoch", err)
	}
	if _, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 7, NodeID: "node-a", NodeEpoch: 3, Sequence: 2, Count: 1,
	}); !errors.Is(err, ErrGlobalTimestampOracleSequenceGap) {
		t.Fatalf("new epoch gap error = %v, want sequence gap", err)
	}
	third, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 7, NodeID: "node-a", NodeEpoch: 3, Sequence: 1, Count: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if third.Start != 4 {
		t.Fatalf("new epoch grant = %#v, want start 4", third)
	}
}

func TestGlobalTimestampOracleTermAndSnapshot(t *testing.T) {
	oracle, err := NewGlobalTimestampOracle(1, 20)
	if err != nil {
		t.Fatal(err)
	}
	first, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 1, NodeID: "node-b", NodeEpoch: 1, Sequence: 1, Count: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.AdvanceTerm(0); !errors.Is(err, ErrGlobalTimestampOracleInvalid) {
		t.Fatalf("zero term error = %v, want invalid", err)
	}
	if err := oracle.AdvanceTerm(1); err != nil {
		t.Fatalf("same term error = %v", err)
	}
	if err := oracle.AdvanceTerm(2); err != nil {
		t.Fatal(err)
	}
	if err := oracle.AdvanceTerm(1); !errors.Is(err, ErrGlobalTimestampOracleStaleTerm) {
		t.Fatalf("stale term error = %v, want stale term", err)
	}
	if _, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 2, NodeID: "node-b", NodeEpoch: 1, Sequence: 1, Count: 2,
	}); !errors.Is(err, ErrGlobalTimestampOracleTermMismatch) {
		t.Fatalf("old grant retry in new term error = %v, want term mismatch", err)
	}
	if _, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1,
	}); !errors.Is(err, ErrGlobalTimestampOracleTermMismatch) {
		t.Fatalf("old term request error = %v, want term mismatch", err)
	}
	second, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 2, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Start != 23 {
		t.Fatalf("new term grant = %#v, want start 23", second)
	}

	snapshot := oracle.Snapshot()
	if len(snapshot.Nodes) != 2 || snapshot.Nodes[0].NodeID != "node-a" || snapshot.Nodes[1].NodeID != "node-b" {
		t.Fatalf("snapshot nodes = %#v, want deterministic node order", snapshot.Nodes)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var decoded GlobalTimestampOracleSnapshot
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, snapshot) {
		t.Fatalf("JSON snapshot = %#v, want %#v", decoded, snapshot)
	}
	restored, err := NewGlobalTimestampOracleFromSnapshot(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot(); len(got.Nodes) != len(snapshot.Nodes) || got.Current != snapshot.Current || got.Term != snapshot.Term {
		t.Fatalf("restored snapshot = %#v, want %#v", got, snapshot)
	}
	retry, err := restored.Reserve(GlobalTimestampRequest{
		Term: 2, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1,
	})
	if err != nil || retry != second {
		t.Fatalf("restored retry = %#v, %v, want %#v", retry, err, second)
	}
	if _, err := NewGlobalTimestampOracleFromSnapshot(GlobalTimestampOracleSnapshot{
		Term: 2, Current: 1,
		Nodes: []GlobalTimestampNodeSnapshot{{NodeID: "node-a", NodeEpoch: 1, Sequence: 1}},
	}); !errors.Is(err, ErrGlobalTimestampOracleInvalid) {
		t.Fatalf("invalid snapshot error = %v, want invalid", err)
	}
	if _, err := NewGlobalTimestampOracleFromSnapshot(GlobalTimestampOracleSnapshot{
		Term: 1, Current: 4,
		Nodes: []GlobalTimestampNodeSnapshot{
			{
				NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Grant: GlobalTimestampGrant{
					Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Start: 1, End: 3, Count: 3,
				},
			},
			{
				NodeID: "node-b", NodeEpoch: 1, Sequence: 1, Grant: GlobalTimestampGrant{
					Term: 1, NodeID: "node-b", NodeEpoch: 1, Sequence: 1, Start: 3, End: 4, Count: 2,
				},
			},
		},
	}); !errors.Is(err, ErrGlobalTimestampOracleInvalid) {
		t.Fatalf("overlapping snapshot error = %v, want invalid", err)
	}
	if first.Start != 21 || first.End != 22 {
		t.Fatalf("first grant changed = %#v, want [21,22]", first)
	}
}

func TestGlobalTimestampOracleConcurrentNodeReservationsDoNotOverlap(t *testing.T) {
	oracle, err := NewGlobalTimestampOracle(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	const nodes = 8
	grants := make(chan GlobalTimestampGrant, nodes)
	errorsCh := make(chan error, nodes)
	var group sync.WaitGroup
	group.Add(nodes)
	for node := 0; node < nodes; node++ {
		node := node
		go func() {
			defer group.Done()
			grant, err := oracle.Reserve(GlobalTimestampRequest{
				Term: 1, NodeID: "node-" + string(rune('a'+node)), NodeEpoch: 1, Sequence: 1, Count: 4,
			})
			if err != nil {
				errorsCh <- err
				return
			}
			grants <- grant
		}()
	}
	group.Wait()
	close(grants)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
	got := make([]GlobalTimestampGrant, 0, nodes)
	for grant := range grants {
		got = append(got, grant)
	}
	sort.Slice(got, func(left, right int) bool { return got[left].Start < got[right].Start })
	for index, grant := range got {
		wantStart := int64(index*4 + 1)
		if grant.Start != wantStart || grant.End != wantStart+3 {
			t.Fatalf("grant %d = %#v, want [%d,%d]", index, grant, wantStart, wantStart+3)
		}
	}
}

func TestGlobalTimestampLeaseConcurrentNextIsUnique(t *testing.T) {
	grant := GlobalTimestampGrant{Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Start: 1, End: 512, Count: 512}
	lease, err := NewGlobalTimestampLease(grant)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 8
	const perWorker = 64
	values := make(chan int64, workers*perWorker)
	var group sync.WaitGroup
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer group.Done()
			for index := 0; index < perWorker; index++ {
				value, ok := lease.Next()
				if !ok {
					t.Errorf("lease.Next() unexpectedly exhausted at %d", index)
					return
				}
				values <- value
			}
		}()
	}
	group.Wait()
	close(values)
	got := make([]int64, 0, workers*perWorker)
	for value := range values {
		got = append(got, value)
	}
	sort.Slice(got, func(left, right int) bool { return got[left] < got[right] })
	for index, value := range got {
		if value != int64(index+1) {
			t.Fatalf("lease value %d = %d, want %d", index, value, index+1)
		}
	}
	if lease.Remaining() != 0 {
		t.Fatalf("lease.Remaining() = %d, want 0", lease.Remaining())
	}
}

func TestGlobalTimestampOracleRejectsInvalidAndOverflow(t *testing.T) {
	if _, err := NewGlobalTimestampOracle(0, 0); !errors.Is(err, ErrGlobalTimestampOracleInvalid) {
		t.Fatalf("zero constructor term error = %v, want invalid", err)
	}
	if _, err := NewGlobalTimestampOracle(1, -1); !errors.Is(err, ErrGlobalTimestampOracleInvalid) {
		t.Fatalf("negative constructor current error = %v, want invalid", err)
	}
	var nilOracle *GlobalTimestampOracle
	if _, err := nilOracle.Reserve(GlobalTimestampRequest{}); !errors.Is(err, ErrGlobalTimestampOracleInvalid) {
		t.Fatalf("nil Reserve() error = %v, want invalid", err)
	}
	if err := nilOracle.AdvanceTerm(1); !errors.Is(err, ErrGlobalTimestampOracleInvalid) {
		t.Fatalf("nil AdvanceTerm() error = %v, want invalid", err)
	}
	for name, request := range map[string]GlobalTimestampRequest{
		"zero term":     {NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1},
		"empty node":    {Term: 1, NodeEpoch: 1, Sequence: 1, Count: 1},
		"zero epoch":    {Term: 1, NodeID: "node-a", Sequence: 1, Count: 1},
		"zero count":    {Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1},
		"negative seen": {Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Observed: -1, Count: 1},
	} {
		t.Run(name, func(t *testing.T) {
			oracle, err := NewGlobalTimestampOracle(1, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := oracle.Reserve(request); !errors.Is(err, ErrGlobalTimestampOracleInvalid) {
				t.Fatalf("Reserve() error = %v, want invalid", err)
			}
		})
	}
	oracle, err := NewGlobalTimestampOracle(1, timestampOracleMax)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oracle.Reserve(GlobalTimestampRequest{
		Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1,
	}); !errors.Is(err, ErrGlobalTimestampOracleOverflow) {
		t.Fatalf("overflow Reserve() error = %v, want overflow", err)
	}
	if got := oracle.Current(); got != timestampOracleMax {
		t.Fatalf("overflow changed current = %d, want %d", got, timestampOracleMax)
	}
	atomicOracle, err := NewGlobalTimestampOracle(1, timestampOracleMax-1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := atomicOracle.Reserve(GlobalTimestampRequest{
		Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Observed: timestampOracleMax, Count: 2,
	}); !errors.Is(err, ErrGlobalTimestampOracleOverflow) {
		t.Fatalf("observed overflow Reserve() error = %v, want overflow", err)
	}
	if got := atomicOracle.Current(); got != timestampOracleMax-1 {
		t.Fatalf("observed overflow changed current = %d, want %d", got, timestampOracleMax-1)
	}
	if _, err := NewGlobalTimestampLease(GlobalTimestampGrant{Start: 0, End: 1}); !errors.Is(err, ErrGlobalTimestampOracleInvalid) {
		t.Fatalf("invalid lease error = %v, want invalid", err)
	}
}
