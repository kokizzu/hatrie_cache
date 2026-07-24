package hatriecache

import (
	"reflect"
	"runtime"
	"testing"
	"time"
	"unsafe"
)

func TestPriorityQueueItemCompactLayout(t *testing.T) {
	if got := unsafe.Sizeof(priorityQueueItem{}); got != 48 {
		t.Fatalf("priorityQueueItem size = %d, want 48", got)
	}
}

func TestPriorityQueueTaggedValuesPreserveBehavior(t *testing.T) {
	nested := Map{"state": "stored"}
	queue := newPriorityQueueData(PriorityQueue{
		{Priority: 1, Value: "first"},
		{Priority: 1, Value: ""},
		{Priority: 1, Value: nil},
		{Priority: 1, Value: nested},
	})
	nested["state"] = "caller"
	if !queue.items[1].hasStringValue() || queue.items[1].stringValue != "" || queue.items[1].Value != priorityQueueEmptyStringValue {
		t.Fatalf("empty string item = %#v, want pre-boxed empty string", queue.items[1])
	}

	snapshot := queue.SnapshotItems()
	if len(snapshot) != 4 {
		t.Fatalf("SnapshotItems() length = %d, want 4", len(snapshot))
	}
	want := []interface{}{"first", "", nil, Map{"state": "stored"}}
	for index, item := range snapshot {
		if item.stringValue != "" {
			t.Fatalf("SnapshotItems()[%d] retained private string representation: %#v", index, item)
		}
		if !reflect.DeepEqual(item.Value, want[index]) {
			t.Fatalf("SnapshotItems()[%d].Value = %#v, want %#v", index, item.Value, want[index])
		}
	}

	for index, expected := range want {
		item, ok := queue.Pop()
		if !ok || item.Priority != 1 || !reflect.DeepEqual(item.Value, expected) {
			t.Fatalf("Pop()[%d] = %#v/%v, want priority 1 value %#v", index, item, ok, expected)
		}
	}
}

func BenchmarkPriorityQueueItemLayout100k(b *testing.B) {
	const itemCount = 100000
	values := make(PriorityQueue, itemCount)
	for index := range values {
		values[index] = PriorityItem{Priority: int64(itemCount - index), Value: "value"}
	}

	var retainedBytes uint64
	var retainedObjects uint64
	var elapsed time.Duration
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		ht := CreateHatTrie()
		b.StartTimer()
		started := time.Now()
		ht.UpsertPriorityQueue("queue", values)
		elapsed += time.Since(started)
		b.StopTimer()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		if after.HeapAlloc > before.HeapAlloc {
			retainedBytes += after.HeapAlloc - before.HeapAlloc
		}
		if after.HeapObjects > before.HeapObjects {
			retainedObjects += after.HeapObjects - before.HeapObjects
		}
		hval := ht.Get("queue")
		if hval.Index < 0 || len(ht.priorityQueues.array[hval.Index].items) != itemCount {
			ht.Destroy()
			b.Fatalf("stored priority queue length = %d, want %d", len(ht.priorityQueues.array[hval.Index].items), itemCount)
		}
		runtime.KeepAlive(ht)
		ht.Destroy()
		b.StartTimer()
	}
	b.StopTimer()
	operations := float64(b.N * itemCount)
	b.ReportMetric(float64(elapsed.Nanoseconds())/operations, "ns/item")
	b.ReportMetric(float64(retainedBytes)/operations, "retained_B/item")
	b.ReportMetric(float64(retainedObjects)/operations, "retained_objects/item")
	b.ReportMetric(float64(unsafe.Sizeof(priorityQueueItem{})), "struct_B/item")
}

var priorityQueueTagBenchmarkSink interface{}

func BenchmarkPriorityQueueTagOperations(b *testing.B) {
	b.Run("StringPushPop", func(b *testing.B) {
		queue := priorityQueueData{items: make([]priorityQueueItem, 0, 1)}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := queue.PushStringChecked(1, "value"); err != nil {
				b.Fatal(err)
			}
			if item, ok := queue.popItemRetain(); !ok || item.value() != "value" {
				b.Fatalf("popItemRetain() = %#v/%v", item, ok)
			}
		}
	})

	b.Run("StringValue", func(b *testing.B) {
		item := newPriorityQueueItem(1, 0, "value")
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			priorityQueueTagBenchmarkSink = item.value()
		}
	})

	b.Run("EmptyStringPushPop", func(b *testing.B) {
		queue := priorityQueueData{items: make([]priorityQueueItem, 0, 1)}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := queue.PushStringChecked(1, ""); err != nil {
				b.Fatal(err)
			}
			if item, ok := queue.popItemRetain(); !ok || item.value() != "" {
				b.Fatalf("popItemRetain() = %#v/%v", item, ok)
			}
		}
	})

	b.Run("GenericValue", func(b *testing.B) {
		item := newPriorityQueueItem(1, 0, int64(42))
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			priorityQueueTagBenchmarkSink = item.value()
		}
	})
}
