package hatSql

import (
	"context"
	"testing"
)

func TestCDCEventsAndIdempotency(t *testing.T) {
	log := NewChangeLog()
	if err := log.Append(ChangeEvent{Namespace: "eu", Operation: "SET", Key: "user:1"}); err != nil {
		t.Fatal(err)
	}
	events := log.After(0)
	if len(events) != 1 || events[0].Sequence != 1 {
		t.Fatalf("events = %#v", events)
	}
	called := 0
	keys := NewIdempotencyKeys()
	for range []int{1, 2} {
		if _, err := keys.Execute(context.Background(), "request-1", func() (interface{}, error) { called++; return "ok", nil }); err != nil {
			t.Fatal(err)
		}
	}
	if called != 1 {
		t.Fatalf("idempotent calls = %d", called)
	}
}
