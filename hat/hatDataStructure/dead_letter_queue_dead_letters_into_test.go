package hatDataStructure

import (
	"reflect"
	"testing"
	"time"
)

func TestDeadLetterQueueDeadLettersIntoMatchesAndReusesDestination(t *testing.T) {
	now := time.Unix(4000, 0)
	queue := NewDeadLetterQueue[int](0, 4)
	for index := 0; index < 3; index++ {
		queue.FailAt(DelayQueueItem[int]{ReadyAt: now, Value: index}, now, 1, "retry")
	}

	destination := make([]DeadLetterItem[int], 0, queue.DeadLetterLen())
	backing := &destination[:cap(destination)][0]
	got := queue.DeadLettersInto(destination)
	if want := queue.DeadLetters(); !reflect.DeepEqual(got, want) {
		t.Fatalf("DeadLettersInto() = %#v, want %#v", got, want)
	}
	if &got[:cap(got)][0] != backing {
		t.Fatal("DeadLettersInto() did not reuse the destination backing array")
	}

	got[0].Reason = "caller mutation"
	if dead, ok := queue.DeadLetter(1); !ok || dead.Reason != "retry" {
		t.Fatalf("DeadLettersInto() exposed queue storage: %#v/%v", dead, ok)
	}

	queue.Clear()
	got = queue.DeadLettersInto(got)
	if len(got) != 0 || cap(got) != cap(destination) {
		t.Fatalf("empty DeadLettersInto() = len %d cap %d, want len 0 cap %d", len(got), cap(got), cap(destination))
	}
}
