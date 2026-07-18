package hatriecache

import "testing"

func TestPriorityQueueStringPushUsesTypedSlot(t *testing.T) {
	var queue priorityQueueData
	if err := queue.PushStringChecked(7, "build"); err != nil {
		t.Fatalf("PushStringChecked() error = %v", err)
	}
	if len(queue.items) != 1 || !queue.items[0].hasString || queue.items[0].stringValue != "build" || queue.items[0].Value != nil {
		t.Fatalf("stored item = %#v, want unboxed typed string", queue.items)
	}
	item, ok := queue.popItemRetain()
	if !ok || item.value() != "build" {
		t.Fatalf("popItemRetain() = %#v/%v, want build/true", item, ok)
	}
}
