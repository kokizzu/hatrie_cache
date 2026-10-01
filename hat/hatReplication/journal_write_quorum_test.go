package hatReplication

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestJournalWriteQuorumStateCountsLocalAndRemoteAcknowledgements(t *testing.T) {
	state, err := NewJournalWriteQuorumState(42, 2, 3)
	if err != nil {
		t.Fatalf("NewJournalWriteQuorumState() error = %v", err)
	}
	decision, err := state.Decision()
	if !errors.Is(err, ErrWriteQuorumUnsatisfied) || decision.Acknowledged != 1 || decision.Required != 2 {
		t.Fatalf("initial Decision() = %#v/%v, want one local acknowledgement and unsatisfied quorum", decision, err)
	}
	state, err = state.Acknowledge(1)
	if err != nil {
		t.Fatalf("Acknowledge(1) error = %v", err)
	}
	state, err = state.Acknowledge(1)
	if err != nil {
		t.Fatalf("duplicate Acknowledge(1) error = %v", err)
	}
	decision, err = state.Decision()
	if err != nil || !decision.Satisfied || decision.Acknowledged != 2 {
		t.Fatalf("duplicate acknowledgement Decision() = %#v/%v, want satisfied with two acknowledgements", decision, err)
	}
	state, err = state.Acknowledge(2)
	if err != nil {
		t.Fatalf("Acknowledge(2) error = %v", err)
	}
	if got := state.Acknowledged(); got != 3 {
		t.Fatalf("Acknowledged() = %d, want 3", got)
	}
}

func TestJournalWriteQuorumStateValidatesBoundsAndJSON(t *testing.T) {
	for _, test := range []struct {
		sequence     uint64
		required     int
		participants int
		want         error
	}{
		{required: 1, participants: 0, want: ErrJournalWriteQuorumInvalid},
		{required: 0, participants: 1, want: ErrJournalWriteQuorumInvalid},
		{required: 2, participants: 1, want: ErrJournalWriteQuorumInvalid},
		{required: 1, participants: 65, want: ErrJournalWriteQuorumInvalid},
	} {
		if _, err := NewJournalWriteQuorumState(test.sequence, test.required, test.participants); !errors.Is(err, test.want) {
			t.Fatalf("NewJournalWriteQuorumState(%d,%d,%d) error = %v, want %v", test.sequence, test.required, test.participants, err, test.want)
		}
	}

	state, err := NewJournalWriteQuorumState(7, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	state, err = state.Acknowledge(2)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var restored JournalWriteQuorumState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if restored != state {
		t.Fatalf("JSON round trip = %#v, want %#v", restored, state)
	}
	if _, err := state.Acknowledge(3); !errors.Is(err, ErrJournalWriteQuorumInvalid) {
		t.Fatalf("Acknowledge(out of range) error = %v, want ErrJournalWriteQuorumInvalid", err)
	}
	maxState, err := NewJournalWriteQuorumState(8, 2, 64)
	if err != nil {
		t.Fatal(err)
	}
	maxState, err = maxState.Acknowledge(63)
	if err != nil {
		t.Fatalf("Acknowledge(63) error = %v", err)
	}
	if decision, err := maxState.Decision(); err != nil || !decision.Satisfied {
		t.Fatalf("64-participant Decision() = %#v/%v, want satisfied", decision, err)
	}
}
