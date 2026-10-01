package hatReplication

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
)

const maxJournalWriteQuorumParticipants = 64

var ErrJournalWriteQuorumInvalid = errors.New("hatriecache: journal write quorum state is invalid")

// JournalWriteQuorumState is the bounded per-journal-record acknowledgement
// state used by a synchronous write-quorum caller. Participant zero is the
// local durable journal write; remote participants occupy indexes 1 through
// Participants-1. The state is copyable and duplicate acknowledgements are
// idempotent.
type JournalWriteQuorumState struct {
	Sequence         uint64 `json:"sequence"`
	Required         uint8  `json:"required"`
	Participants     uint8  `json:"participants"`
	AcknowledgedMask uint64 `json:"acknowledged_mask"`
}

// NewJournalWriteQuorumState creates a state with the local journal write
// already acknowledged. A caller keeps the state for one journal sequence,
// acknowledges remote participants as they complete, and waits for Decision
// to become satisfied before reporting the write successful.
func NewJournalWriteQuorumState(sequence uint64, required, participants int) (JournalWriteQuorumState, error) {
	state := JournalWriteQuorumState{
		Sequence:         sequence,
		Required:         uint8(required),
		Participants:     uint8(participants),
		AcknowledgedMask: 1,
	}
	if err := validateJournalWriteQuorumBounds(sequence, required, participants); err != nil {
		return JournalWriteQuorumState{}, err
	}
	return state, nil
}

// Validate checks a state loaded from durable or wire storage.
func (state JournalWriteQuorumState) Validate() error {
	if err := validateJournalWriteQuorumBounds(state.Sequence, int(state.Required), int(state.Participants)); err != nil {
		return err
	}
	if state.AcknowledgedMask == 0 || state.AcknowledgedMask&^journalWriteQuorumParticipantMask(state.Participants) != 0 {
		return fmt.Errorf("%w: acknowledgement mask %#x is outside %d participants", ErrJournalWriteQuorumInvalid, state.AcknowledgedMask, state.Participants)
	}
	return nil
}

// Acknowledge records one participant. Repeating an acknowledgement is a
// no-op, which makes retries safe when a transport response is duplicated.
func (state JournalWriteQuorumState) Acknowledge(participant int) (JournalWriteQuorumState, error) {
	if err := state.Validate(); err != nil {
		return JournalWriteQuorumState{}, err
	}
	if participant < 0 || participant >= int(state.Participants) {
		return JournalWriteQuorumState{}, fmt.Errorf("%w: participant=%d participants=%d", ErrJournalWriteQuorumInvalid, participant, state.Participants)
	}
	state.AcknowledgedMask |= uint64(1) << uint(participant)
	return state, nil
}

// Acknowledged returns the number of unique acknowledged participants.
func (state JournalWriteQuorumState) Acknowledged() int {
	return bits.OnesCount64(state.AcknowledgedMask)
}

// Decision evaluates the existing write-quorum policy for this journal
// sequence. It returns the decision together with ErrWriteQuorumUnsatisfied
// until the required number of participants has acknowledged.
func (state JournalWriteQuorumState) Decision() (WriteQuorumDecision, error) {
	if err := state.Validate(); err != nil {
		return WriteQuorumDecision{}, err
	}
	return EvaluateWriteQuorum(int(state.Participants), state.Acknowledged(), int(state.Required))
}

func (state JournalWriteQuorumState) MarshalJSON() ([]byte, error) {
	if err := state.Validate(); err != nil {
		return nil, err
	}
	type journalWriteQuorumState JournalWriteQuorumState
	return json.Marshal(journalWriteQuorumState(state))
}

func (state *JournalWriteQuorumState) UnmarshalJSON(data []byte) error {
	type journalWriteQuorumState JournalWriteQuorumState
	var decoded journalWriteQuorumState
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	loaded := JournalWriteQuorumState(decoded)
	if err := loaded.Validate(); err != nil {
		return err
	}
	*state = loaded
	return nil
}

func validateJournalWriteQuorumBounds(sequence uint64, required, participants int) error {
	if sequence == 0 {
		return fmt.Errorf("%w: journal sequence must be positive", ErrJournalWriteQuorumInvalid)
	}
	if participants < 1 || participants > maxJournalWriteQuorumParticipants {
		return fmt.Errorf("%w: participants=%d outside [1,%d]", ErrJournalWriteQuorumInvalid, participants, maxJournalWriteQuorumParticipants)
	}
	if required < 1 || required > participants {
		return fmt.Errorf("%w: required=%d participants=%d", ErrJournalWriteQuorumInvalid, required, participants)
	}
	return nil
}

func journalWriteQuorumParticipantMask(participants uint8) uint64 {
	if participants == maxJournalWriteQuorumParticipants {
		return ^uint64(0)
	}
	return (uint64(1) << participants) - 1
}
