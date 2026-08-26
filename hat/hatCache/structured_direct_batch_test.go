package hatCache

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestStructuredBatchDirectMatchesGenericBatchForEveryOperation(t *testing.T) {
	request := testStructuredBatchRequest(701)
	referenceTrie := newTestTrie(t)
	directTrie := newTestTrie(t)
	fixedNow := func() time.Time { return time.Unix(1700000000, 0) }
	referenceTrie.now = fixedNow
	directTrie.now = fixedNow

	referenceResult := referenceTrie.ExecuteCommand(structuredBatchCacheCommand(request))
	reference := structuredBatchResponseFromCommand(request, referenceResult)
	direct := directTrie.executeStructuredBatchDirect(context.Background(), request)

	if !reflect.DeepEqual(direct, reference) {
		t.Fatalf("direct structured batch = %#v, want generic %#v", direct, reference)
	}
	if directTrie.Stats() != referenceTrie.Stats() {
		t.Fatalf("direct structured stats = %#v, want generic %#v", directTrie.Stats(), referenceTrie.Stats())
	}
	if !reflect.DeepEqual(directTrie.Entries(true), referenceTrie.Entries(true)) {
		t.Fatalf("direct structured entries = %#v, want generic %#v", directTrie.Entries(true), referenceTrie.Entries(true))
	}
}

func TestStructuredBatchDirectAggregatesDefaultTelemetryClock(t *testing.T) {
	trie := newTestTrie(t)
	request := structuredBenchmarkRequest(702, 0, 16)
	nowCalls := 0
	trie.now = func() time.Time {
		nowCalls++
		return time.Unix(1700000000, 0)
	}

	response := trie.executeStructuredBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetStatuses()) != len(request.GetOperations()) {
		t.Fatalf("executeStructuredBatchDirect() = %#v", response)
	}
	if nowCalls != 1 {
		t.Fatalf("structured batch clock calls = %d, want 1", nowCalls)
	}
}

func TestStructuredBatchDirectPreservesCommandLoopFallbacks(t *testing.T) {
	t.Run("detailed key stats", func(t *testing.T) {
		request := testStructuredBatchRequest(703)
		referenceTrie := newTestTrie(t)
		directTrie := newTestTrie(t)
		for _, trie := range []*HatTrie{referenceTrie, directTrie} {
			if err := trie.ConfigureKeyStats(KeyStatsModeFull, 0); err != nil {
				t.Fatal(err)
			}
			trie.now = func() time.Time { return time.Unix(1700000001, 0) }
		}
		if !directTrie.structuredBatchRequiresCommandLoop(request) {
			t.Fatal("detailed key stats did not select the command-loop fallback")
		}
		reference := structuredBatchResponseFromCommand(request, referenceTrie.ExecuteCommand(structuredBatchCacheCommand(request)))
		direct := directTrie.executeStructuredBatchDirect(context.Background(), request)
		if !reflect.DeepEqual(direct, reference) || directTrie.Stats() != referenceTrie.Stats() {
			t.Fatalf("detailed fallback response/stats = %#v/%#v, want %#v/%#v", direct, directTrie.Stats(), reference, referenceTrie.Stats())
		}
	})

	t.Run("trimmed key and subkey", func(t *testing.T) {
		request := structuredBenchmarkRequest(704, 0, 2)
		request.Keys[0] = " structured:map "
		request.Subkeys[0] = " field "
		referenceTrie := newTestTrie(t)
		directTrie := newTestTrie(t)
		if !directTrie.structuredBatchRequiresCommandLoop(request) {
			t.Fatal("trimmed input did not select the command-loop fallback")
		}
		reference := structuredBatchResponseFromCommand(request, referenceTrie.ExecuteCommand(structuredBatchCacheCommand(request)))
		direct := directTrie.executeStructuredBatchDirect(context.Background(), request)
		if !reflect.DeepEqual(direct, reference) || !reflect.DeepEqual(directTrie.Entries(true), referenceTrie.Entries(true)) {
			t.Fatalf("trimmed fallback response/entries = %#v/%#v, want %#v/%#v", direct, directTrie.Entries(true), reference, referenceTrie.Entries(true))
		}
	})

	t.Run("local partitions", func(t *testing.T) {
		request := testStructuredBatchRequest(705)
		referenceTrie := newTestTrie(t)
		directTrie := newTestTrie(t)
		for _, trie := range []*HatTrie{referenceTrie, directTrie} {
			if err := trie.ConfigureLocalPartitions(8); err != nil {
				t.Fatal(err)
			}
		}
		if !directTrie.structuredBatchRequiresCommandLoop(request) {
			t.Fatal("local partitions did not select the command-loop fallback")
		}
		reference := structuredBatchResponseFromCommand(request, referenceTrie.ExecuteCommand(structuredBatchCacheCommand(request)))
		direct := directTrie.executeStructuredBatchDirect(context.Background(), request)
		if !reflect.DeepEqual(direct, reference) {
			t.Fatalf("partition fallback response = %#v, want %#v", direct, reference)
		}
	})
}

func TestStructuredBatchBoundedPreservesStatsAcrossTelemetryModeChange(t *testing.T) {
	request := structuredBenchmarkRequest(706, 0, 8)
	expectedTrie := newTestTrie(t)
	directTrie := newTestTrie(t)
	fixedNow := func() time.Time { return time.Unix(1700000002, 0) }
	expectedTrie.now = fixedNow
	directTrie.now = fixedNow

	expectedResult := expectedTrie.ExecuteCommand(structuredBatchCacheCommand(request))
	expected := structuredBatchResponseFromCommand(request, expectedResult)
	transitionContext := &structuredBatchTransitionContext{
		Context: context.Background(),
		onSecondCheck: func() {
			if err := directTrie.ConfigureKeyStats(KeyStatsModeFull, 0); err != nil {
				t.Fatal(err)
			}
		},
	}
	direct := directTrie.executeStructuredBatchBoundedWithChunkSize(transitionContext, request, 4)

	if !reflect.DeepEqual(direct, expected) {
		t.Fatalf("telemetry transition response = %#v, want %#v", direct, expected)
	}
	if directTrie.Stats() != expectedTrie.Stats() {
		t.Fatalf("telemetry transition stats = %#v, want %#v", directTrie.Stats(), expectedTrie.Stats())
	}
}

type structuredBatchTransitionContext struct {
	context.Context
	checks        int
	onSecondCheck func()
}

func (ctx *structuredBatchTransitionContext) Err() error {
	ctx.checks++
	if ctx.checks == 2 {
		ctx.onSecondCheck()
	}
	return nil
}
