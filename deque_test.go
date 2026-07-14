package hatriecache

import (
	"errors"
	"reflect"
	"testing"
)

func TestDequeWraparoundMaintainsOrder(t *testing.T) {
	dq := newDeque(Slice{0, 1, 2, 3})

	for _, want := range (Slice{0, 1}) {
		got, ok := dq.Shift()
		if !ok || got != want {
			t.Fatalf("Shift() = %v, %v; want %v, true", got, ok, want)
		}
	}
	dq.Push(4, 5, 6)

	if got := dq.Slice(); !reflect.DeepEqual(got, Slice{2, 3, 4, 5, 6}) {
		t.Fatalf("Slice() after wraparound = %#v, want %#v", got, Slice{2, 3, 4, 5, 6})
	}
	if got, ok := dq.Pop(); !ok || got != 6 {
		t.Fatalf("Pop() = %v, %v; want 6, true", got, ok)
	}
	if got, ok := dq.Head(); !ok || got != 2 {
		t.Fatalf("Head() = %v, %v; want 2, true", got, ok)
	}
	if got, ok := dq.Tail(); !ok || got != 5 {
		t.Fatalf("Tail() = %v, %v; want 5, true", got, ok)
	}
	if got := dq.Slice(); !reflect.DeepEqual(got, Slice{2, 3, 4, 5}) {
		t.Fatalf("Slice() = %#v, want %#v", got, Slice{2, 3, 4, 5})
	}
}

func TestDequeClearsRemovedSlots(t *testing.T) {
	first := &struct{ name string }{name: "first"}
	last := &struct{ name string }{name: "last"}
	dq := newDeque(Slice{first, "middle", last})

	if got, ok := dq.Shift(); !ok || got != first {
		t.Fatalf("Shift() = %v, %v; want first, true", got, ok)
	}
	for idx, value := range dq.values {
		if value == first {
			t.Fatalf("Shift() retained removed value at slot %d", idx)
		}
	}

	if got, ok := dq.Pop(); !ok || got != last {
		t.Fatalf("Pop() = %v, %v; want last, true", got, ok)
	}
	for idx, value := range dq.values {
		if value == last {
			t.Fatalf("Pop() retained removed value at slot %d", idx)
		}
	}
}

func TestDequeReleasesBackingArrayWhenDrained(t *testing.T) {
	dq := newDeque(Slice{"a", "b"})

	if _, ok := dq.Shift(); !ok {
		t.Fatal("Shift() failed")
	}
	if _, ok := dq.Pop(); !ok {
		t.Fatal("Pop() failed")
	}

	if got := dq.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
	if len(dq.values) != 0 {
		t.Fatalf("drained deque retained backing array length %d, want 0", len(dq.values))
	}
	if got := dq.Slice(); got == nil || len(got) != 0 {
		t.Fatalf("Slice() after drain = %#v, want non-nil empty slice", got)
	}
}

func TestDequeGrowthRejectsCapacityOverflow(t *testing.T) {
	capacity, ok := grownDequeCapacity(2, 5)
	if !ok || capacity != 8 {
		t.Fatalf("grownDequeCapacity(2, 5) = %d/%v, want 8/true", capacity, ok)
	}

	max := int(^uint(0) >> 1)
	if capacity, ok := grownDequeCapacity(max/2+1, max); ok {
		t.Fatalf("grownDequeCapacity(overflow) = %d/true, want rejection", capacity)
	}
	if _, ok := checkedDequeNeeded(max, 1); ok {
		t.Fatal("checkedDequeNeeded(max, 1) ok = true, want overflow rejection")
	}

	dq := deque{size: max}
	if err := dq.PushOneChecked("value"); !errors.Is(err, errDequeCapacityTooLarge) {
		t.Fatalf("PushOneChecked(overflow) error = %v, want errDequeCapacityTooLarge", err)
	}
	if got := dq.size; got != max {
		t.Fatalf("PushOneChecked(overflow) size = %d, want unchanged %d", got, max)
	}
}

func TestSliceQueueOperationsMaintainOrderAfterDequeWraparound(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertSlice("queue", Slice{0, 1, 2, 3})

	for _, want := range (Slice{0, 1}) {
		if got := ht.ShiftSlice("queue"); got != want {
			t.Fatalf("ShiftSlice() = %v, want %v", got, want)
		}
	}
	ht.PushSlice("queue", 4, 5, 6)

	if got := ht.PopSlice("queue"); got != 6 {
		t.Fatalf("PopSlice() = %v, want 6", got)
	}
	if got := ht.HeadSlice("queue"); got != 2 {
		t.Fatalf("HeadSlice() = %v, want 2", got)
	}
	if got := ht.TailSlice("queue"); got != 5 {
		t.Fatalf("TailSlice() = %v, want 5", got)
	}
	if got := ht.GetSlice("queue"); !reflect.DeepEqual(got, Slice{2, 3, 4, 5}) {
		t.Fatalf("GetSlice() = %#v, want %#v", got, Slice{2, 3, 4, 5})
	}
}
