package hatReplication

import (
	"errors"
	"sync"
	"testing"
)

func TestChangefeedFrontierEmitsMonotonicProgress(t *testing.T) {
	frontier := NewChangefeedFrontier(10)
	initial, err := frontier.Progress()
	if err != nil {
		t.Fatalf("Progress() error = %v", err)
	}
	if initial.Sequence != 10 || !initial.Progressed {
		t.Fatalf("initial progress = %#v, want sequence 10 and progressed", initial)
	}
	advanced, err := frontier.Advance(12)
	if err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	if advanced.Sequence != 12 || !advanced.Progressed || frontier.Current() != 12 {
		t.Fatalf("advanced progress = %#v/current=%d, want sequence 12", advanced, frontier.Current())
	}
	equal, err := frontier.Advance(12)
	if err != nil || equal != advanced {
		t.Fatalf("equal Advance() = %#v, %v, want idempotent progress", equal, err)
	}
	if _, err := frontier.Advance(11); !errors.Is(err, ErrChangefeedFrontierRegressed) {
		t.Fatalf("regressed Advance() error = %v, want ErrChangefeedFrontierRegressed", err)
	}
	var nilFrontier *ChangefeedFrontier
	if _, err := nilFrontier.Progress(); !errors.Is(err, ErrChangefeedFrontierNil) {
		t.Fatalf("nil Progress() error = %v, want ErrChangefeedFrontierNil", err)
	}
}

func TestChangefeedFrontierConcurrentAdvanceKeepsMaximum(t *testing.T) {
	frontier := NewChangefeedFrontier(1)
	var group sync.WaitGroup
	for sequence := uint64(2); sequence <= 64; sequence++ {
		sequence := sequence
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := frontier.Advance(sequence); err != nil && !errors.Is(err, ErrChangefeedFrontierRegressed) {
				t.Errorf("Advance(%d) error = %v", sequence, err)
			}
		}()
	}
	group.Wait()
	if got := frontier.Current(); got != 64 {
		t.Fatalf("Current() = %d, want maximum 64", got)
	}
}
