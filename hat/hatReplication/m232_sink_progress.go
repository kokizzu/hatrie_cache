package hatReplication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
)

const (
	MaxSinkProgressRecords       = 4096
	MaxSinkProgressEnvelopeBytes = 4 << 20
	sinkProgressEnvelopeVersion  = 1
)

var (
	ErrSinkProgressInvalid         = errors.New("hatriecache: sink progress envelope is invalid")
	ErrSinkProgressRecordsRequired = errors.New("hatriecache: sink progress output records are required")
	ErrSinkProgressRegressed       = errors.New("hatriecache: sink progress frontier regressed")
	ErrSinkProgressOverlap         = errors.New("hatriecache: sink progress output sequence overlaps")
)

// SinkProgressEnvelope couples output records and the source frontier that is
// safe to acknowledge after those records are durably emitted.
type SinkProgressEnvelope struct {
	Source   string                        `json:"source"`
	Frontier uint64                        `json:"frontier"`
	Records  []ExactlyOnceUpsertSinkRecord `json:"records"`
}

// SinkProgressEmitter constructs monotone envelopes for one source. It never
// emits a frontier without at least one output record in the same envelope.
type SinkProgressEmitter struct {
	mu       sync.RWMutex
	source   string
	frontier uint64
}

// NewSinkProgressEmitter creates an emitter at initial frontier.
func NewSinkProgressEmitter(source string, initial uint64) (*SinkProgressEmitter, error) {
	if validateExactlyOnceUpsertSinkIdentity(source) != nil {
		return nil, ErrSinkProgressInvalid
	}
	return &SinkProgressEmitter{source: source, frontier: initial}, nil
}

// Emit returns one immutable output-plus-frontier envelope. A frontier may
// jump over filtered source sequences, but every output record must be newer
// than the emitter frontier and no record may exceed the emitted frontier.
func (emitter *SinkProgressEmitter) Emit(records []ExactlyOnceUpsertSinkRecord, frontier uint64) (SinkProgressEnvelope, error) {
	if emitter == nil {
		return SinkProgressEnvelope{}, ErrSinkProgressInvalid
	}
	if len(records) == 0 {
		return SinkProgressEnvelope{}, ErrSinkProgressRecordsRequired
	}
	if frontier == 0 {
		return SinkProgressEnvelope{}, ErrSinkProgressInvalid
	}
	emitter.mu.Lock()
	defer emitter.mu.Unlock()
	if frontier <= emitter.frontier {
		return SinkProgressEnvelope{}, ErrSinkProgressRegressed
	}
	envelope := SinkProgressEnvelope{
		Source:   emitter.source,
		Frontier: frontier,
		Records:  cloneSinkProgressRecords(records),
	}
	if err := validateSinkProgressEnvelope(envelope); err != nil {
		return SinkProgressEnvelope{}, err
	}
	for _, record := range records {
		if record.Sequence <= emitter.frontier {
			return SinkProgressEnvelope{}, ErrSinkProgressOverlap
		}
	}
	emitter.frontier = frontier
	return envelope, nil
}

// Progress returns the current monotone frontier message.
func (emitter *SinkProgressEmitter) Progress() ChangefeedProgress {
	if emitter == nil {
		return ChangefeedProgress{}
	}
	emitter.mu.RLock()
	defer emitter.mu.RUnlock()
	return ChangefeedProgress{Sequence: emitter.frontier, Progressed: true}
}

// MarshalBinary encodes one coupled output/frontier envelope in compact SPG1.
func (envelope SinkProgressEnvelope) MarshalBinary() ([]byte, error) {
	if err := validateSinkProgressEnvelope(envelope); err != nil {
		return nil, err
	}
	size := 4 + 1 + 1 + 2 + len(envelope.Source) + 8 + 4
	for _, record := range envelope.Records {
		size += 8 + 2 + len(record.OutputID) + 4 + len(record.Key) + 4 + len(record.Value) + 1
	}
	if size > MaxSinkProgressEnvelopeBytes {
		return nil, ErrSinkProgressInvalid
	}
	var buffer bytes.Buffer
	buffer.Grow(size)
	buffer.WriteString("spg1")
	buffer.WriteByte(sinkProgressEnvelopeVersion)
	buffer.WriteByte(0)
	writeExactlyOnceUpsertSinkString(&buffer, envelope.Source)
	writeExactlyOnceUpsertSinkUint64(&buffer, envelope.Frontier)
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(envelope.Records)))
	buffer.Write(count[:])
	for _, record := range envelope.Records {
		writeExactlyOnceUpsertSinkUint64(&buffer, record.Sequence)
		writeExactlyOnceUpsertSinkString(&buffer, record.OutputID)
		writeExactlyOnceUpsertSinkBytes(&buffer, record.Key)
		writeExactlyOnceUpsertSinkBytes(&buffer, record.Value)
		if record.Delete {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
	}
	return buffer.Bytes(), nil
}

// UnmarshalBinary decodes a coupled output/frontier envelope.
func (envelope *SinkProgressEnvelope) UnmarshalBinary(data []byte) error {
	if envelope == nil {
		return ErrSinkProgressInvalid
	}
	decoded, err := UnmarshalSinkProgressEnvelope(data)
	if err != nil {
		return err
	}
	*envelope = decoded
	return nil
}

// UnmarshalSinkProgressEnvelope decodes and validates SPG1 without changing
// caller-owned bytes.
func UnmarshalSinkProgressEnvelope(data []byte) (SinkProgressEnvelope, error) {
	if len(data) < 6 || len(data) > MaxSinkProgressEnvelopeBytes || string(data[:4]) != "spg1" || data[4] != sinkProgressEnvelopeVersion || data[5] != 0 {
		return SinkProgressEnvelope{}, ErrSinkProgressInvalid
	}
	reader := exactlyOnceUpsertSinkReader{data: data, offset: 6}
	envelope := SinkProgressEnvelope{}
	var ok bool
	if envelope.Source, ok = reader.string(); !ok {
		return SinkProgressEnvelope{}, ErrSinkProgressInvalid
	}
	if envelope.Frontier, ok = reader.uint64(); !ok {
		return SinkProgressEnvelope{}, ErrSinkProgressInvalid
	}
	if reader.offset+4 > len(reader.data) {
		return SinkProgressEnvelope{}, ErrSinkProgressInvalid
	}
	count := binary.BigEndian.Uint32(reader.data[reader.offset : reader.offset+4])
	reader.offset += 4
	if count == 0 || count > MaxSinkProgressRecords {
		return SinkProgressEnvelope{}, ErrSinkProgressInvalid
	}
	envelope.Records = make([]ExactlyOnceUpsertSinkRecord, int(count))
	for index := range envelope.Records {
		record := &envelope.Records[index]
		if record.Sequence, ok = reader.uint64(); !ok {
			return SinkProgressEnvelope{}, ErrSinkProgressInvalid
		}
		if record.OutputID, ok = reader.string(); !ok {
			return SinkProgressEnvelope{}, ErrSinkProgressInvalid
		}
		if record.Key, ok = reader.bytes(MaxExactlyOnceUpsertSinkKeyBytes); !ok {
			return SinkProgressEnvelope{}, ErrSinkProgressInvalid
		}
		if record.Value, ok = reader.bytes(MaxExactlyOnceUpsertSinkValueBytes); !ok || reader.offset >= len(reader.data) {
			return SinkProgressEnvelope{}, ErrSinkProgressInvalid
		}
		deleteFlag := reader.data[reader.offset]
		reader.offset++
		if deleteFlag > 1 {
			return SinkProgressEnvelope{}, ErrSinkProgressInvalid
		}
		record.Delete = deleteFlag == 1
	}
	if reader.offset != len(reader.data) || validateSinkProgressEnvelope(envelope) != nil {
		return SinkProgressEnvelope{}, ErrSinkProgressInvalid
	}
	return envelope, nil
}

func validateSinkProgressEnvelope(envelope SinkProgressEnvelope) error {
	if validateExactlyOnceUpsertSinkIdentity(envelope.Source) != nil || envelope.Frontier == 0 || len(envelope.Records) == 0 || len(envelope.Records) > MaxSinkProgressRecords {
		if len(envelope.Records) == 0 {
			return ErrSinkProgressRecordsRequired
		}
		return ErrSinkProgressInvalid
	}
	var previous uint64
	for index, record := range envelope.Records {
		if validateExactlyOnceUpsertSinkRecord(record) != nil || record.Sequence > envelope.Frontier || (index > 0 && record.Sequence <= previous) {
			return ErrSinkProgressInvalid
		}
		previous = record.Sequence
	}
	return nil
}

func cloneSinkProgressRecords(records []ExactlyOnceUpsertSinkRecord) []ExactlyOnceUpsertSinkRecord {
	cloned := make([]ExactlyOnceUpsertSinkRecord, len(records))
	for index, record := range records {
		cloned[index] = cloneExactlyOnceUpsertSinkRecord(record)
	}
	return cloned
}
