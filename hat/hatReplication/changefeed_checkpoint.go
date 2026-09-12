package hatReplication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrChangefeedCheckpointInvalid        = errors.New("hatriecache: changefeed checkpoint is invalid")
	ErrChangefeedCheckpointSourceRequired = errors.New("hatriecache: changefeed checkpoint source is required")
	ErrChangefeedProgressInvalid          = errors.New("hatriecache: changefeed progress is invalid")
	ErrChangefeedCheckpointRegressed      = errors.New("hatriecache: changefeed checkpoint regressed")
)

const (
	MaxChangefeedCheckpointSourceBytes = 256
	changefeedCheckpointHeaderBytes    = 4 + 2 + 8
	MaxChangefeedCheckpointBytes       = changefeedCheckpointHeaderBytes + MaxChangefeedCheckpointSourceBytes
	encodedHeaderSourceOffset          = changefeedCheckpointHeaderBytes
)

var changefeedCheckpointMagic = [4]byte{'h', 'c', 'p', '1'}

// ChangefeedCheckpoint identifies the last progress frontier durably applied
// by a consumer for one source.
type ChangefeedCheckpoint struct {
	Source   string `json:"source"`
	Sequence uint64 `json:"sequence"`
}

// NewChangefeedCheckpoint creates a validated source-bound checkpoint.
func NewChangefeedCheckpoint(source string, sequence uint64) (ChangefeedCheckpoint, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return ChangefeedCheckpoint{}, ErrChangefeedCheckpointSourceRequired
	}
	if len(source) > MaxChangefeedCheckpointSourceBytes {
		return ChangefeedCheckpoint{}, fmt.Errorf("%w: source exceeds %d bytes", ErrChangefeedCheckpointInvalid, MaxChangefeedCheckpointSourceBytes)
	}
	return ChangefeedCheckpoint{Source: source, Sequence: sequence}, nil
}

// Advance returns a checkpoint after applying a progress message from the
// same source. Equal progress is idempotent; regressed progress is rejected.
func (checkpoint ChangefeedCheckpoint) Advance(progress ChangefeedProgress) (ChangefeedCheckpoint, error) {
	validated, err := NewChangefeedCheckpoint(checkpoint.Source, checkpoint.Sequence)
	if err != nil {
		return ChangefeedCheckpoint{}, err
	}
	if !progress.Progressed {
		return ChangefeedCheckpoint{}, ErrChangefeedProgressInvalid
	}
	if progress.Sequence < validated.Sequence {
		return ChangefeedCheckpoint{}, fmt.Errorf("%w: current=%d requested=%d", ErrChangefeedCheckpointRegressed, validated.Sequence, progress.Sequence)
	}
	validated.Sequence = progress.Sequence
	return validated, nil
}

// MarshalBinary encodes a bounded, deterministic checkpoint frame. The
// encoded source is copied into the frame and is not caller-owned afterward.
func (checkpoint ChangefeedCheckpoint) MarshalBinary() ([]byte, error) {
	validated, err := NewChangefeedCheckpoint(checkpoint.Source, checkpoint.Sequence)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, changefeedCheckpointHeaderBytes+len(validated.Source))
	copy(encoded, changefeedCheckpointMagic[:])
	binary.BigEndian.PutUint16(encoded[4:6], uint16(len(validated.Source)))
	binary.BigEndian.PutUint64(encoded[6:14], validated.Sequence)
	copy(encoded[encodedHeaderSourceOffset:], validated.Source)
	return encoded, nil
}

// UnmarshalChangefeedCheckpoint validates and owns a decoded checkpoint.
func UnmarshalChangefeedCheckpoint(encoded []byte) (ChangefeedCheckpoint, error) {
	if len(encoded) < changefeedCheckpointHeaderBytes || len(encoded) > MaxChangefeedCheckpointBytes {
		return ChangefeedCheckpoint{}, ErrChangefeedCheckpointInvalid
	}
	if string(encoded[:len(changefeedCheckpointMagic)]) != string(changefeedCheckpointMagic[:]) {
		return ChangefeedCheckpoint{}, ErrChangefeedCheckpointInvalid
	}
	sourceLength := int(binary.BigEndian.Uint16(encoded[4:6]))
	if sourceLength == 0 || sourceLength > MaxChangefeedCheckpointSourceBytes || len(encoded) != changefeedCheckpointHeaderBytes+sourceLength {
		return ChangefeedCheckpoint{}, ErrChangefeedCheckpointInvalid
	}
	source := string(encoded[encodedHeaderSourceOffset:])
	if strings.TrimSpace(source) != source {
		return ChangefeedCheckpoint{}, ErrChangefeedCheckpointInvalid
	}
	return ChangefeedCheckpoint{Source: source, Sequence: binary.BigEndian.Uint64(encoded[6:14])}, nil
}
