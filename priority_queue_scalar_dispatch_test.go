package hatriecache

import (
	"reflect"
	"testing"
	"time"
)

var benchmarkPriorityQueueScalarSink priorityQueueItem

func TestPriorityQueueScalarPushCandidateMatchesReference(t *testing.T) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	values := []interface{}{
		"value",
		"",
		int64(42),
		structuredValue{Name: "structured"},
		Map{"name": "nested"},
	}
	for _, value := range values {
		candidate := newPriorityQueueData(PriorityQueue{{Priority: 5, Value: "seed"}})
		reference := newPriorityQueueData(PriorityQueue{{Priority: 5, Value: "seed"}})
		got, gotErr := priorityQueueScalarPushCandidate(&candidate, 1, value)
		want, wantErr := priorityQueueScalarPushReference(&reference, 1, value)
		if gotErr != nil || wantErr != nil || got != want {
			t.Fatalf("scalar push %#v = %d/%v, want %d/%v", value, got, gotErr, want, wantErr)
		}
		if !reflect.DeepEqual(candidate, reference) {
			t.Fatalf("scalar push %#v state differs\ncandidate: %#v\nreference: %#v", value, candidate, reference)
		}
	}
}

func TestPriorityQueueScalarPushCandidatePreservesSequenceExhaustion(t *testing.T) {
	candidate := priorityQueueData{nextSequence: ^uint64(0)}
	reference := candidate
	got, gotErr := priorityQueueScalarPushCandidate(&candidate, 1, "value")
	want, wantErr := priorityQueueScalarPushReference(&reference, 1, "value")
	if gotErr != errPriorityQueueSequenceExhausted || wantErr != errPriorityQueueSequenceExhausted || got != want {
		t.Fatalf("exhausted scalar push = %d/%v, want %d/%v", got, gotErr, want, wantErr)
	}
	if !reflect.DeepEqual(candidate, reference) {
		t.Fatalf("exhausted scalar push mutated state: candidate %#v reference %#v", candidate, reference)
	}
}

func TestPriorityQueueScalarPushPublicBehavior(t *testing.T) {
	ht := newTestTrie(t)
	original := Map{"name": "stored"}
	if added, err := ht.PushPriorityQueueChecked("queue", 1, original); err != nil || added != 1 {
		t.Fatalf("PushPriorityQueueChecked(map) = %d/%v, want 1/nil", added, err)
	}
	original["name"] = "caller"
	if added, err := ht.PushPriorityQueueChecked("queue", 1, "second"); err != nil || added != 1 {
		t.Fatalf("PushPriorityQueueChecked(string) = %d/%v, want 1/nil", added, err)
	}
	first, ok, err := ht.PopPriorityQueueChecked("queue")
	if err != nil || !ok || first.Priority != 1 || !reflect.DeepEqual(first.Value, Map{"name": "stored"}) {
		t.Fatalf("first PopPriorityQueueChecked = %#v/%v/%v", first, ok, err)
	}
	second, ok, err := ht.PopPriorityQueueChecked("queue")
	if err != nil || !ok || second.Priority != 1 || second.Value != "second" {
		t.Fatalf("second PopPriorityQueueChecked = %#v/%v/%v", second, ok, err)
	}

	ht.UpsertString("replace", "old")
	if added, err := ht.PushPriorityQueueChecked("replace", 2, "new"); err != nil || added != 1 {
		t.Fatalf("PushPriorityQueueChecked(replace) = %d/%v, want 1/nil", added, err)
	}
	if item, ok := ht.PopPriorityQueue("replace"); !ok || item.Value != "new" {
		t.Fatalf("PopPriorityQueue(replace) = %#v/%v, want new/true", item, ok)
	}
}

func BenchmarkPriorityQueueScalarPushAlternating(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name  string
		value interface{}
	}{
		{name: "String", value: "value"},
		{name: "Struct", value: structuredValue{Name: "value"}},
		{name: "Map", value: Map{"name": "value"}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			candidate := priorityQueueData{items: make([]priorityQueueItem, 0, 1)}
			reference := priorityQueueData{items: make([]priorityQueueItem, 0, 1)}
			const operationsPerBlock = 128
			var candidateDuration time.Duration
			var referenceDuration time.Duration
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if iteration&1 == 0 {
					candidateDuration += benchmarkPriorityQueueScalarCandidateBlock(b, &candidate, benchmark.value, operationsPerBlock)
					referenceDuration += benchmarkPriorityQueueScalarReferenceBlock(b, &reference, benchmark.value, operationsPerBlock)
				} else {
					referenceDuration += benchmarkPriorityQueueScalarReferenceBlock(b, &reference, benchmark.value, operationsPerBlock)
					candidateDuration += benchmarkPriorityQueueScalarCandidateBlock(b, &candidate, benchmark.value, operationsPerBlock)
				}
			}
			b.StopTimer()
			operations := float64(b.N * operationsPerBlock)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/op")
			b.ReportMetric(float64(referenceDuration.Nanoseconds())/operations, "reference-ns/op")
		})
	}
}

func BenchmarkPriorityQueueScalarPushAllocations(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name      string
		value     interface{}
		candidate bool
	}{
		{name: "StringReference", value: "value"},
		{name: "StringCandidate", value: "value", candidate: true},
		{name: "StructReference", value: structuredValue{Name: "value"}},
		{name: "StructCandidate", value: structuredValue{Name: "value"}, candidate: true},
		{name: "MapReference", value: Map{"name": "value"}},
		{name: "MapCandidate", value: Map{"name": "value"}, candidate: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			queue := priorityQueueData{items: make([]priorityQueueItem, 0, 1)}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				var err error
				if benchmark.candidate {
					_, err = priorityQueueScalarPushCandidate(&queue, 1, benchmark.value)
				} else {
					_, err = priorityQueueScalarPushReference(&queue, 1, benchmark.value)
				}
				if err != nil {
					b.Fatal(err)
				}
				benchmarkPriorityQueueScalarSink, _ = queue.popItemRetain()
			}
		})
	}
}

func BenchmarkPriorityQueueScalarPushProductionControls(b *testing.B) {
	type structuredValue struct {
		Name string `json:"name"`
	}
	for _, benchmark := range []struct {
		name    string
		value   interface{}
		values  []interface{}
		missing bool
	}{
		{name: "ExistingString", value: "value"},
		{name: "ExistingStructured", value: structuredValue{Name: "value"}},
		{name: "MissingString", value: "value", missing: true},
		{name: "MissingBatch2", value: "value-0", values: priorityQueueScalarStrings(1, 2), missing: true},
		{name: "MissingBatch16", value: "value-0", values: priorityQueueScalarStrings(1, 16), missing: true},
		{name: "Batch2", value: "value-0", values: priorityQueueScalarStrings(1, 2)},
		{name: "Batch16", value: "value-0", values: priorityQueueScalarStrings(1, 16)},
		{name: "Batch128", value: "value-0", values: priorityQueueScalarStrings(1, 128)},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			defer ht.Destroy()
			var queue *priorityQueueData
			if !benchmark.missing {
				if added, err := ht.PushPriorityQueueChecked("queue", 100, "seed"); err != nil || added != 1 {
					b.Fatalf("setup push = %d/%v", added, err)
				}
				hval := ht.Get("queue")
				queue = &ht.priorityQueues.array[hval.Index]
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				added, err := ht.PushPriorityQueueChecked("queue", 1, benchmark.value, benchmark.values...)
				if err != nil || added != 1+len(benchmark.values) {
					b.Fatalf("PushPriorityQueueChecked = %d/%v", added, err)
				}
				if benchmark.missing {
					if !ht.Delete("queue") {
						b.Fatal("Delete(queue) = false")
					}
					continue
				}
				for count := 0; count < added; count++ {
					benchmarkPriorityQueueScalarSink, _ = queue.popItemRetain()
				}
			}
		})
	}
}

func priorityQueueScalarStrings(start int, end int) []interface{} {
	values := make([]interface{}, 0, end-start)
	for index := start; index < end; index++ {
		values = append(values, "value")
	}
	return values
}

func benchmarkPriorityQueueScalarCandidateBlock(b *testing.B, queue *priorityQueueData, value interface{}, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		if _, err := priorityQueueScalarPushCandidate(queue, 1, value); err != nil {
			b.Fatal(err)
		}
		benchmarkPriorityQueueScalarSink, _ = queue.popItemRetain()
	}
	return time.Since(start)
}

func benchmarkPriorityQueueScalarReferenceBlock(b *testing.B, queue *priorityQueueData, value interface{}, operations int) time.Duration {
	start := time.Now()
	for operation := 0; operation < operations; operation++ {
		if _, err := priorityQueueScalarPushReference(queue, 1, value); err != nil {
			b.Fatal(err)
		}
		benchmarkPriorityQueueScalarSink, _ = queue.popItemRetain()
	}
	return time.Since(start)
}

func priorityQueueScalarPushCandidate(queue *priorityQueueData, priority int64, value interface{}) (int, error) {
	count, ok := checkedBatchSize(1, 0)
	if !ok {
		return 0, errBatchSizeTooLarge
	}
	if err := queue.ensureSequenceCapacity(count); err != nil {
		return 0, err
	}
	if _, ok := checkedBatchSize(len(queue.items), count); !ok {
		return 0, errBatchSizeTooLarge
	}
	item := priorityQueueItem{Priority: priority, Sequence: queue.nextSequence}
	if text, ok := value.(string); ok {
		item = newPriorityQueueStringItem(priority, queue.nextSequence, text)
	} else {
		item.Value = cloneValue(value)
	}
	queue.nextSequence++
	queue.items = append(queue.items, item)
	queue.siftUp(len(queue.items) - 1)
	return 1, nil
}

func priorityQueueScalarPushReference(queue *priorityQueueData, priority int64, value interface{}) (int, error) {
	count, ok := checkedBatchSize(1, 0)
	if !ok {
		return 0, errBatchSizeTooLarge
	}
	if err := queue.ensureSequenceCapacity(count); err != nil {
		return 0, err
	}
	if _, ok := checkedBatchSize(len(queue.items), count); !ok {
		return 0, errBatchSizeTooLarge
	}
	queue.pushValue(priority, value)
	return count, nil
}
