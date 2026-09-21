package hatReplication_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestM209ChangefeedFrontierPreservesMonotoneTimestampContract(t *testing.T) {
	frontier := hatReplication.NewChangefeedFrontier(7)
	if progress, err := frontier.Advance(9); err != nil || progress.Sequence != 9 {
		t.Fatalf("forward Advance() = %#v/%v, want sequence 9", progress, err)
	}
	if progress, err := frontier.Advance(8); !errors.Is(err, hatReplication.ErrChangefeedFrontierRegressed) || progress.Sequence != 9 {
		t.Fatalf("regressed Advance() = %#v/%v, want sequence 9/regression", progress, err)
	}
	if !frontier.AtLeast(9) {
		t.Fatal("AtLeast(9) = false, want true")
	}
}
