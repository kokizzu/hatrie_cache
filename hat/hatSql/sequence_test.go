package hatSql

import (
	"reflect"
	"testing"
	"time"
)

func TestMatchOrderedEventSequenceByKeyAndGap(t *testing.T) {
	t0 := time.Unix(0, 0).UTC()
	events := []OrderedEvent{
		{Key: "u2", Kind: "view", At: t0},
		{Key: "u1", Kind: "purchase", At: t0.Add(2 * time.Minute)},
		{Key: "u1", Kind: "view", At: t0},
		{Key: "u2", Kind: "purchase", At: t0.Add(2 * time.Minute)},
		{Key: "u1", Kind: "add", At: t0.Add(time.Minute)},
		{Key: "u2", Kind: "add", At: t0.Add(time.Minute)},
		{Key: "u1", Kind: "view", At: t0.Add(10 * time.Minute)},
		{Key: "u1", Kind: "add", At: t0.Add(20 * time.Minute)},
		{Key: "u1", Kind: "purchase", At: t0.Add(21 * time.Minute)},
	}

	matches, err := MatchOrderedEventSequence(events, []string{"view", "add", "purchase"}, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("matches = %#v", matches)
	}
	if got := []string{matches[0].Key, matches[1].Key}; !reflect.DeepEqual(got, []string{"u1", "u2"}) {
		t.Fatalf("match keys = %v", got)
	}
	if got := []string{matches[0].Events[0].Kind, matches[0].Events[1].Kind, matches[0].Events[2].Kind}; !reflect.DeepEqual(got, []string{"view", "add", "purchase"}) {
		t.Fatalf("first match kinds = %v", got)
	}
	if events[0].Key != "u2" {
		t.Fatal("MatchOrderedEventSequence mutated caller event order")
	}
}

func TestMatchOrderedEventSequenceValidationAndContiguity(t *testing.T) {
	t0 := time.Unix(0, 0).UTC()
	events := []OrderedEvent{
		{Key: "u1", Kind: "view", At: t0},
		{Key: "u1", Kind: "noise", At: t0.Add(time.Minute)},
		{Key: "u1", Kind: "purchase", At: t0.Add(2 * time.Minute)},
	}
	matches, err := MatchOrderedEventSequence(events, []string{"view", "purchase"}, 0)
	if err != nil || len(matches) != 0 {
		t.Fatalf("contiguous matching = %#v, %v", matches, err)
	}
	if _, err := MatchOrderedEventSequence(events, nil, 0); err == nil {
		t.Fatal("accepted empty pattern")
	}
	if _, err := MatchOrderedEventSequence(events, []string{"view"}, -time.Second); err == nil {
		t.Fatal("accepted negative maximum gap")
	}
	if _, err := MatchOrderedEventSequence([]OrderedEvent{{Kind: "view", At: t0}}, []string{"view"}, 0); err == nil {
		t.Fatal("accepted event without key")
	}
}
