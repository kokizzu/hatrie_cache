package hatDataStructure

import "testing"

func TestDeduplicatingQueueRejectsActiveDuplicatesAndPreservesOrder(t *testing.T) {
	queue := NewDeduplicatingQueue[string, int](4)
	if !queue.Enqueue("a", 1) {
		t.Fatal("first a enqueue rejected")
	}
	if queue.Enqueue("a", 2) {
		t.Fatal("active duplicate accepted")
	}
	if !queue.Enqueue("b", 3) {
		t.Fatal("b enqueue rejected")
	}
	if !queue.Contains("a") || !queue.Contains("b") || queue.Len() != 2 {
		t.Fatalf("queue state: contains a=%v b=%v len=%d", queue.Contains("a"), queue.Contains("b"), queue.Len())
	}

	key, value, ok := queue.Dequeue()
	if !ok || key != "a" || value != 1 {
		t.Fatalf("first dequeue = %q/%d/%v, want a/1/true", key, value, ok)
	}
	if queue.Contains("a") {
		t.Fatal("dequeued key still marked active")
	}
	if !queue.Enqueue("a", 4) {
		t.Fatal("re-enqueue after dequeue rejected")
	}
	key, value, ok = queue.Dequeue()
	if !ok || key != "b" || value != 3 {
		t.Fatalf("second dequeue = %q/%d/%v, want b/3/true", key, value, ok)
	}
	key, value, ok = queue.Dequeue()
	if !ok || key != "a" || value != 4 {
		t.Fatalf("third dequeue = %q/%d/%v, want a/4/true", key, value, ok)
	}
	if _, _, ok := queue.Dequeue(); ok {
		t.Fatal("empty dequeue succeeded")
	}
}

func TestDeduplicatingQueueClearReleasesActiveKeys(t *testing.T) {
	queue := NewDeduplicatingQueue[int, string](1)
	for index := 0; index < 2048; index++ {
		if !queue.Enqueue(index, "value") {
			t.Fatalf("enqueue %d rejected", index)
		}
	}
	queue.Clear()
	if queue.Len() != 0 || queue.Contains(1) {
		t.Fatalf("cleared queue state: len=%d contains=%v", queue.Len(), queue.Contains(1))
	}
	if !queue.Enqueue(1, "again") {
		t.Fatal("enqueue after clear rejected")
	}
}
