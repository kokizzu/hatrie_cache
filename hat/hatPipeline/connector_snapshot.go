package hatPipeline

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"
	"time"
)

const (
	connectorSnapshotHeaderSize   = 5
	connectorSnapshotChecksumSize = 4
	maxConnectorSnapshotBytes     = 64 << 20
	maxConnectorSnapshotIDs       = 4096
	maxConnectorSnapshotEvents    = 1 << 18
	maxConnectorSnapshotIDBytes   = 1 << 20
	maxConnectorSnapshotErrBytes  = 1 << 20
)

var (
	// ErrConnectorSnapshotInvalid indicates malformed or semantically invalid
	// connector checkpoint data.
	ErrConnectorSnapshotInvalid = errors.New("hatPipeline: connector snapshot is invalid")
	// ErrConnectorSnapshotNotEmpty indicates that restore would overwrite
	// already-registered connector state.
	ErrConnectorSnapshotNotEmpty = errors.New("hatPipeline: connector snapshot requires an empty registry")
	// ErrConnectorSnapshotConnectorMismatch indicates that the caller did not
	// provide exactly the connector implementations named by the checkpoint.
	ErrConnectorSnapshotConnectorMismatch = errors.New("hatPipeline: connector snapshot implementations do not match")
)

var connectorSnapshotMagic = [4]byte{'H', 'C', 'S', '1'}

var connectorSnapshotCRCTable = crc32.MakeTable(crc32.Castagnoli)

// ConnectorSnapshot is the durable metadata for one connector. Connector
// implementations are intentionally not included; callers provide fresh
// implementations when restoring a registry.
type ConnectorSnapshot struct {
	Status ConnectorStatus
	Events []ConnectorEvent
}

// ConnectorRegistrySnapshot is a detached, deterministic checkpoint of the
// connector control plane. It contains no runtime pointers, channels, or
// callback state.
type ConnectorRegistrySnapshot struct {
	HistoryLimit int
	Connectors   []ConnectorSnapshot
}

// SnapshotState returns connector statuses and bounded transition history in
// ID order. The returned slices are detached from the live registry.
func (r *ConnectorRegistry) SnapshotState() ConnectorRegistrySnapshot {
	if r == nil {
		return ConnectorRegistrySnapshot{}
	}
	r.mu.RLock()
	entries := make([]*managedConnector, 0, len(r.connectors))
	for _, entry := range r.connectors {
		entries = append(entries, entry)
	}
	historyLimit := r.historyLimit
	r.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })

	snapshot := ConnectorRegistrySnapshot{
		HistoryLimit: historyLimit,
		Connectors:   make([]ConnectorSnapshot, 0, len(entries)),
	}
	for _, entry := range entries {
		entry.mu.Lock()
		snapshot.Connectors = append(snapshot.Connectors, ConnectorSnapshot{
			Status: entry.status,
			Events: connectorEventsLocked(entry, historyLimit),
		})
		entry.mu.Unlock()
	}
	return snapshot
}

// MarshalSnapshot encodes the connector control plane as a bounded,
// deterministic versioned binary checkpoint. The caller owns durable storage
// and should publish the returned bytes atomically.
func (r *ConnectorRegistry) MarshalSnapshot() ([]byte, error) {
	if r == nil {
		return nil, ErrConnectorSnapshotInvalid
	}
	return marshalConnectorRegistrySnapshot(r.SnapshotState())
}

// UnmarshalConnectorRegistrySnapshot decodes and validates a connector
// checkpoint without creating or starting any connector implementations.
func UnmarshalConnectorRegistrySnapshot(payload []byte) (ConnectorRegistrySnapshot, error) {
	if len(payload) < connectorSnapshotHeaderSize+connectorSnapshotChecksumSize || len(payload) > maxConnectorSnapshotBytes {
		return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
	}
	body := payload[:len(payload)-connectorSnapshotChecksumSize]
	if !bytes.Equal(body[:4], connectorSnapshotMagic[:]) || body[4] != 1 || binary.LittleEndian.Uint32(payload[len(payload)-connectorSnapshotChecksumSize:]) != crc32.Checksum(body, connectorSnapshotCRCTable) {
		return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
	}
	offset := connectorSnapshotHeaderSize
	historyLimit, ok := readConnectorSnapshotUvarint(body, &offset)
	if !ok || historyLimit > maxConnectorHistoryLimit {
		return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
	}
	count, ok := readConnectorSnapshotUvarint(body, &offset)
	if !ok || count > maxConnectorSnapshotIDs {
		return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
	}

	snapshot := ConnectorRegistrySnapshot{
		HistoryLimit: int(historyLimit),
		Connectors:   make([]ConnectorSnapshot, 0, int(count)),
	}
	totalEvents := 0
	for index := uint64(0); index < count; index++ {
		id, ok := readConnectorSnapshotString(body, &offset, maxConnectorSnapshotIDBytes)
		if !ok {
			return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
		}
		state, ok := readConnectorSnapshotByte(body, &offset)
		if !ok {
			return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
		}
		generation, ok := readConnectorSnapshotUvarint(body, &offset)
		if !ok {
			return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
		}
		updatedAt, ok := readConnectorSnapshotVarint(body, &offset)
		if !ok {
			return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
		}
		lastError, ok := readConnectorSnapshotString(body, &offset, maxConnectorSnapshotErrBytes)
		if !ok {
			return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
		}
		eventCount, ok := readConnectorSnapshotUvarint(body, &offset)
		if !ok || eventCount > maxConnectorHistoryLimit || eventCount > uint64(maxConnectorSnapshotEvents-totalEvents) {
			return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
		}
		totalEvents += int(eventCount)
		events := make([]ConnectorEvent, 0, int(eventCount))
		for eventIndex := uint64(0); eventIndex < eventCount; eventIndex++ {
			sequence, ok := readConnectorSnapshotUvarint(body, &offset)
			if !ok {
				return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
			}
			eventID, ok := readConnectorSnapshotString(body, &offset, maxConnectorSnapshotIDBytes)
			if !ok {
				return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
			}
			from, ok := readConnectorSnapshotByte(body, &offset)
			if !ok {
				return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
			}
			to, ok := readConnectorSnapshotByte(body, &offset)
			if !ok {
				return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
			}
			eventAt, ok := readConnectorSnapshotVarint(body, &offset)
			if !ok {
				return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
			}
			eventError, ok := readConnectorSnapshotString(body, &offset, maxConnectorSnapshotErrBytes)
			if !ok {
				return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
			}
			events = append(events, ConnectorEvent{
				Sequence: sequence,
				ID:       eventID,
				From:     ConnectorState(from),
				To:       ConnectorState(to),
				At:       connectorSnapshotTime(eventAt),
				Error:    eventError,
			})
		}
		snapshot.Connectors = append(snapshot.Connectors, ConnectorSnapshot{
			Status: ConnectorStatus{
				ID:         id,
				State:      ConnectorState(state),
				Generation: generation,
				UpdatedAt:  connectorSnapshotTime(updatedAt),
				LastError:  lastError,
			},
			Events: events,
		})
	}
	if offset != len(body) || validateConnectorRegistrySnapshot(snapshot) != nil {
		return ConnectorRegistrySnapshot{}, ErrConnectorSnapshotInvalid
	}
	return snapshot, nil
}

// NewConnectorRegistryFromSnapshot constructs an empty registry from a
// validated metadata checkpoint and fresh connector implementations. Restore
// never invokes connector callbacks; the caller decides when to reconcile or
// start work after process recovery.
func NewConnectorRegistryFromSnapshot(snapshot ConnectorRegistrySnapshot, connectors map[string]Connector) (*ConnectorRegistry, error) {
	if err := validateConnectorRegistrySnapshot(snapshot); err != nil {
		return nil, err
	}
	historyLimit := normalizedConnectorHistoryLimit(snapshot.HistoryLimit)
	registry, err := NewConnectorRegistry(ConnectorRegistryOptions{HistoryLimit: historyLimit})
	if err != nil {
		return nil, err
	}
	if err := registry.restoreSnapshot(snapshot, connectors); err != nil {
		return nil, err
	}
	return registry, nil
}

// RestoreSnapshot atomically restores a validated checkpoint into an empty
// registry. The connector map must contain exactly one fresh implementation
// for every checkpointed ID. No connector callbacks are invoked.
func (r *ConnectorRegistry) RestoreSnapshot(payload []byte, connectors map[string]Connector) error {
	if r == nil {
		return ErrConnectorSnapshotInvalid
	}
	snapshot, err := UnmarshalConnectorRegistrySnapshot(payload)
	if err != nil {
		return err
	}
	return r.restoreSnapshot(snapshot, connectors)
}

func (r *ConnectorRegistry) restoreSnapshot(snapshot ConnectorRegistrySnapshot, connectors map[string]Connector) error {
	if err := validateConnectorRegistrySnapshot(snapshot); err != nil {
		return err
	}
	if len(connectors) != len(snapshot.Connectors) {
		return ErrConnectorSnapshotConnectorMismatch
	}

	entries := make(map[string]*managedConnector, len(snapshot.Connectors))
	for _, saved := range snapshot.Connectors {
		connector, ok := connectors[saved.Status.ID]
		if !ok || connector == nil {
			return ErrConnectorSnapshotConnectorMismatch
		}
		events := append([]ConnectorEvent(nil), saved.Events...)
		entries[saved.Status.ID] = &managedConnector{
			id:        saved.Status.ID,
			connector: connector,
			status:    saved.Status,
			events:    events,
			eventNext: len(events) % normalizedConnectorHistoryLimit(snapshot.HistoryLimit),
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrConnectorRegistryClosed
	}
	if len(r.connectors) != 0 {
		return ErrConnectorSnapshotNotEmpty
	}
	r.historyLimit = normalizedConnectorHistoryLimit(snapshot.HistoryLimit)
	r.connectors = entries
	return nil
}

func marshalConnectorRegistrySnapshot(snapshot ConnectorRegistrySnapshot) ([]byte, error) {
	if err := validateConnectorRegistrySnapshot(snapshot); err != nil {
		return nil, err
	}
	connectors := append([]ConnectorSnapshot(nil), snapshot.Connectors...)
	sort.Slice(connectors, func(i, j int) bool { return connectors[i].Status.ID < connectors[j].Status.ID })
	historyLimit := normalizedConnectorHistoryLimit(snapshot.HistoryLimit)
	encodedSize, ok := connectorSnapshotEncodedSize(connectors, historyLimit)
	if !ok {
		return nil, ErrConnectorSnapshotInvalid
	}
	encoded := make([]byte, 0, encodedSize)
	encoded = append(encoded, connectorSnapshotMagic[:]...)
	encoded = append(encoded, 1)
	encoded = appendConnectorSnapshotUvarint(encoded, uint64(historyLimit))
	encoded = appendConnectorSnapshotUvarint(encoded, uint64(len(connectors)))
	for _, saved := range connectors {
		status := saved.Status
		encoded = appendConnectorSnapshotString(encoded, status.ID)
		encoded = append(encoded, byte(status.State))
		encoded = appendConnectorSnapshotUvarint(encoded, status.Generation)
		encoded = appendConnectorSnapshotVarint(encoded, status.UpdatedAt.UnixNano())
		encoded = appendConnectorSnapshotString(encoded, status.LastError)
		encoded = appendConnectorSnapshotUvarint(encoded, uint64(len(saved.Events)))
		for _, event := range saved.Events {
			encoded = appendConnectorSnapshotUvarint(encoded, event.Sequence)
			encoded = appendConnectorSnapshotString(encoded, event.ID)
			encoded = append(encoded, byte(event.From), byte(event.To))
			encoded = appendConnectorSnapshotVarint(encoded, event.At.UnixNano())
			encoded = appendConnectorSnapshotString(encoded, event.Error)
		}
		if len(encoded) > maxConnectorSnapshotBytes {
			return nil, ErrConnectorSnapshotInvalid
		}
	}
	if len(encoded)+connectorSnapshotChecksumSize > maxConnectorSnapshotBytes {
		return nil, ErrConnectorSnapshotInvalid
	}
	checksum := crc32.Checksum(encoded, connectorSnapshotCRCTable)
	var checksumBytes [connectorSnapshotChecksumSize]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	encoded = append(encoded, checksumBytes[:]...)
	return encoded, nil
}

func validateConnectorRegistrySnapshot(snapshot ConnectorRegistrySnapshot) error {
	if snapshot.HistoryLimit < 0 || snapshot.HistoryLimit > maxConnectorHistoryLimit || len(snapshot.Connectors) > maxConnectorSnapshotIDs {
		return ErrConnectorSnapshotInvalid
	}
	historyLimit := normalizedConnectorHistoryLimit(snapshot.HistoryLimit)
	seen := make(map[string]struct{}, len(snapshot.Connectors))
	for _, saved := range snapshot.Connectors {
		status := saved.Status
		if status.ID == "" || len(status.ID) > maxConnectorSnapshotIDBytes || status.State > ConnectorStopped {
			return ErrConnectorSnapshotInvalid
		}
		if _, exists := seen[status.ID]; exists {
			return ErrConnectorSnapshotInvalid
		}
		seen[status.ID] = struct{}{}
		if len(status.LastError) > maxConnectorSnapshotErrBytes || len(saved.Events) > historyLimit {
			return ErrConnectorSnapshotInvalid
		}
		previousSequence := uint64(0)
		for _, event := range saved.Events {
			if event.Sequence == 0 || event.Sequence <= previousSequence || event.Sequence > status.Generation || event.ID != status.ID || len(event.ID) > maxConnectorSnapshotIDBytes || event.From > ConnectorStopped || event.To > ConnectorStopped || len(event.Error) > maxConnectorSnapshotErrBytes {
				return ErrConnectorSnapshotInvalid
			}
			previousSequence = event.Sequence
		}
		if len(saved.Events) > 0 && (saved.Events[len(saved.Events)-1].Sequence != status.Generation || saved.Events[len(saved.Events)-1].To != status.State) {
			return ErrConnectorSnapshotInvalid
		}
	}
	if totalConnectorSnapshotEvents(snapshot) > maxConnectorSnapshotEvents {
		return ErrConnectorSnapshotInvalid
	}
	return nil
}

func normalizedConnectorHistoryLimit(historyLimit int) int {
	if historyLimit == 0 {
		return defaultConnectorHistoryLimit
	}
	return historyLimit
}

func totalConnectorSnapshotEvents(snapshot ConnectorRegistrySnapshot) int {
	total := 0
	for _, saved := range snapshot.Connectors {
		total += len(saved.Events)
	}
	return total
}

func connectorSnapshotEncodedSize(connectors []ConnectorSnapshot, historyLimit int) (int, bool) {
	size := 0
	add := func(amount int) bool {
		if amount < 0 || amount > maxConnectorSnapshotBytes-size {
			return false
		}
		size += amount
		return true
	}
	addUvarint := func(value uint64) bool {
		var encoded [binary.MaxVarintLen64]byte
		return add(binary.PutUvarint(encoded[:], value))
	}
	addVarint := func(value int64) bool {
		var encoded [binary.MaxVarintLen64]byte
		return add(binary.PutVarint(encoded[:], value))
	}
	addString := func(value string) bool {
		return addUvarint(uint64(len(value))) && add(len(value))
	}

	if !add(connectorSnapshotHeaderSize) || !addUvarint(uint64(historyLimit)) || !addUvarint(uint64(len(connectors))) {
		return 0, false
	}
	for _, saved := range connectors {
		status := saved.Status
		if !addString(status.ID) || !add(1) || !addUvarint(status.Generation) || !addVarint(status.UpdatedAt.UnixNano()) || !addString(status.LastError) || !addUvarint(uint64(len(saved.Events))) {
			return 0, false
		}
		for _, event := range saved.Events {
			if !addUvarint(event.Sequence) || !addString(event.ID) || !add(2) || !addVarint(event.At.UnixNano()) || !addString(event.Error) {
				return 0, false
			}
		}
	}
	if !add(connectorSnapshotChecksumSize) {
		return 0, false
	}
	return size, true
}

func connectorEventsLocked(entry *managedConnector, historyLimit int) []ConnectorEvent {
	events := make([]ConnectorEvent, len(entry.events))
	if len(entry.events) == historyLimit && len(entry.events) > 0 {
		copied := copy(events, entry.events[entry.eventNext:])
		copy(events[copied:], entry.events[:entry.eventNext])
		return events
	}
	copy(events, entry.events)
	return events
}

func appendConnectorSnapshotUvarint(payload []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(encoded[:], value)
	return append(payload, encoded[:size]...)
}

func appendConnectorSnapshotVarint(payload []byte, value int64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	size := binary.PutVarint(encoded[:], value)
	return append(payload, encoded[:size]...)
}

func appendConnectorSnapshotString(payload []byte, value string) []byte {
	payload = appendConnectorSnapshotUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func readConnectorSnapshotUvarint(payload []byte, offset *int) (uint64, bool) {
	if *offset >= len(payload) {
		return 0, false
	}
	value, size := binary.Uvarint(payload[*offset:])
	if size <= 0 {
		return 0, false
	}
	*offset += size
	return value, true
}

func readConnectorSnapshotVarint(payload []byte, offset *int) (int64, bool) {
	if *offset >= len(payload) {
		return 0, false
	}
	value, size := binary.Varint(payload[*offset:])
	if size <= 0 {
		return 0, false
	}
	*offset += size
	return value, true
}

func readConnectorSnapshotByte(payload []byte, offset *int) (byte, bool) {
	if *offset >= len(payload) {
		return 0, false
	}
	value := payload[*offset]
	*offset++
	return value, true
}

func readConnectorSnapshotString(payload []byte, offset *int, maxLength int) (string, bool) {
	length, ok := readConnectorSnapshotUvarint(payload, offset)
	if !ok || length > uint64(maxLength) || length > uint64(len(payload)-*offset) {
		return "", false
	}
	end := *offset + int(length)
	value := string(payload[*offset:end])
	*offset = end
	return value, true
}

func connectorSnapshotTime(nanoseconds int64) (timestamp time.Time) {
	return time.Unix(0, nanoseconds).UTC()
}
