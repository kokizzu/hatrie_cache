package hatDataStructure

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
)

const (
	// MaxTupleUpgradeIDBytes bounds the checkpoint identity retained by an
	// upgrade coordinator.
	MaxTupleUpgradeIDBytes = 128
	// MaxTupleUpgradeCheckpointBytes bounds one crash-recovery checkpoint.
	MaxTupleUpgradeCheckpointBytes         = 256
	tupleUpgradeCheckpointWireVersion byte = 1
)

var (
	// ErrTupleUpgradeInvalid indicates an invalid plan, checkpoint, or state.
	ErrTupleUpgradeInvalid = errors.New("hatDataStructure: tuple upgrade is invalid")
	// ErrTupleUpgradeFormatMismatch indicates a row version outside the active
	// upgrade boundary.
	ErrTupleUpgradeFormatMismatch = errors.New("hatDataStructure: tuple upgrade format mismatch")
	// ErrTupleUpgradePhase indicates an invalid state transition.
	ErrTupleUpgradePhase = errors.New("hatDataStructure: tuple upgrade phase transition is invalid")
	// ErrTupleUpgradeIncomplete indicates that migration progress is incomplete.
	ErrTupleUpgradeIncomplete = errors.New("hatDataStructure: tuple upgrade is incomplete")
	// ErrTupleUpgradeConflict indicates that another operation changed progress.
	ErrTupleUpgradeConflict = errors.New("hatDataStructure: tuple upgrade progress changed concurrently")
	// ErrTupleUpgradeConversion indicates a converter callback failure.
	ErrTupleUpgradeConversion = errors.New("hatDataStructure: tuple upgrade conversion failed")
	// ErrTupleUpgradeCheckpointWire indicates malformed checkpoint bytes.
	ErrTupleUpgradeCheckpointWire = errors.New("hatDataStructure: tuple upgrade checkpoint wire is invalid")
)

var tupleUpgradeCheckpointMagic = [4]byte{'H', 'U', 'P', '1'}
var tupleUpgradeCheckpointCRCTable = crc32.MakeTable(crc32.Castagnoli)

// TupleUpgradePhase is the monotone lifecycle of an online format upgrade.
type TupleUpgradePhase uint8

const (
	TupleUpgradePhaseActive TupleUpgradePhase = iota + 1
	TupleUpgradePhaseCutover
	TupleUpgradePhaseComplete
	TupleUpgradePhaseRolledBack
)

func (phase TupleUpgradePhase) String() string {
	switch phase {
	case TupleUpgradePhaseActive:
		return "active"
	case TupleUpgradePhaseCutover:
		return "cutover"
	case TupleUpgradePhaseComplete:
		return "complete"
	case TupleUpgradePhaseRolledBack:
		return "rolled_back"
	default:
		return "unknown"
	}
}

// TupleUpgradeConverter converts one validated row in each direction. Both
// directions are required so a prepared upgrade can be rolled back without
// accepting a row in the wrong physical format.
type TupleUpgradeConverter struct {
	ToNext     func(VersionedTuple) (VersionedTuple, error)
	ToPrevious func(VersionedTuple) (VersionedTuple, error)
}

// TupleUpgradeCheckpoint is the durable progress boundary for one online
// upgrade. A new coordinator with the same formats and converter can restore
// this value after a process restart.
type TupleUpgradeCheckpoint struct {
	ID              string
	PreviousVersion uint64
	NextVersion     uint64
	TotalRows       uint64
	NextRow         uint64
	MigratedRows    uint64
	Generation      uint64
	Phase           TupleUpgradePhase
}

// OnlineTupleUpgrade accepts both formats while migration is active, converts
// old rows in bounded atomic batches, and exposes an explicit cutover and
// rollback boundary. It does not own the table storage; callers persist rows
// and checkpoints around the returned values.
type OnlineTupleUpgrade struct {
	mu sync.RWMutex

	id         string
	previous   TupleFormat
	next       TupleFormat
	converter  TupleUpgradeConverter
	totalRows  uint64
	nextRow    uint64
	migrated   uint64
	generation uint64
	phase      TupleUpgradePhase
}

// NewOnlineTupleUpgrade validates and creates an active online upgrade.
func NewOnlineTupleUpgrade(id string, previous, next TupleFormat, totalRows uint64, converter TupleUpgradeConverter) (*OnlineTupleUpgrade, error) {
	id = strings.TrimSpace(id)
	if id == "" || len(id) > MaxTupleUpgradeIDBytes {
		return nil, fmt.Errorf("%w: upgrade ID is empty or too long", ErrTupleUpgradeInvalid)
	}
	if err := previous.validateDefinition(); err != nil {
		return nil, fmt.Errorf("%w: previous format: %v", ErrTupleUpgradeInvalid, err)
	}
	if err := next.validateDefinition(); err != nil {
		return nil, fmt.Errorf("%w: next format: %v", ErrTupleUpgradeInvalid, err)
	}
	if previous.Version() == next.Version() {
		return nil, fmt.Errorf("%w: format versions must differ", ErrTupleUpgradeInvalid)
	}
	if converter.ToNext == nil || converter.ToPrevious == nil {
		return nil, fmt.Errorf("%w: both conversion callbacks are required", ErrTupleUpgradeInvalid)
	}
	return &OnlineTupleUpgrade{
		id:        id,
		previous:  previous,
		next:      next,
		converter: converter,
		totalRows: totalRows,
		phase:     TupleUpgradePhaseActive,
	}, nil
}

// Phase returns the current lifecycle phase.
func (upgrade *OnlineTupleUpgrade) Phase() TupleUpgradePhase {
	if upgrade == nil {
		return 0
	}
	upgrade.mu.RLock()
	defer upgrade.mu.RUnlock()
	return upgrade.phase
}

// Complete reports whether the next format has been committed at cutover.
func (upgrade *OnlineTupleUpgrade) Complete() bool {
	return upgrade != nil && upgrade.Phase() == TupleUpgradePhaseComplete
}

// Snapshot returns a copy suitable for durable checkpointing.
func (upgrade *OnlineTupleUpgrade) Snapshot() TupleUpgradeCheckpoint {
	if upgrade == nil {
		return TupleUpgradeCheckpoint{}
	}
	upgrade.mu.RLock()
	defer upgrade.mu.RUnlock()
	return upgrade.checkpointLocked()
}

// Read validates a row in the phase's accepted format and converts it to the
// active read format. Returned rows from the already-active format remain
// borrowed like the input VersionedTuple; converted rows are newly packed.
func (upgrade *OnlineTupleUpgrade) Read(tuple VersionedTuple) (VersionedTuple, error) {
	if upgrade == nil {
		return VersionedTuple{}, ErrTupleUpgradeInvalid
	}
	upgrade.mu.RLock()
	phase := upgrade.phase
	previous := upgrade.previous
	next := upgrade.next
	converter := upgrade.converter
	upgrade.mu.RUnlock()
	return upgrade.readAtPhase(phase, previous, next, converter, tuple)
}

// Write packs a new row in the next format during forward operation and the
// previous format after rollback.
func (upgrade *OnlineTupleUpgrade) Write(values []TupleFieldValue) (VersionedTuple, error) {
	if upgrade == nil {
		return VersionedTuple{}, ErrTupleUpgradeInvalid
	}
	upgrade.mu.RLock()
	phase := upgrade.phase
	format := upgrade.next
	if phase == TupleUpgradePhaseRolledBack {
		format = upgrade.previous
	}
	upgrade.mu.RUnlock()
	if phase == 0 {
		return VersionedTuple{}, ErrTupleUpgradeInvalid
	}
	return format.PackVersioned(values)
}

// MigrateBatch converts at most limit rows and commits progress only after all
// conversions succeed. The input is the caller's current window beginning at
// the checkpoint's NextRow. Concurrent batches use an optimistic generation
// check and cannot silently overwrite each other's progress.
func (upgrade *OnlineTupleUpgrade) MigrateBatch(ctx context.Context, rows []VersionedTuple, limit int) ([]VersionedTuple, error) {
	if upgrade == nil {
		return nil, ErrTupleUpgradeInvalid
	}
	if limit <= 0 {
		return nil, fmt.Errorf("%w: batch limit must be positive", ErrTupleUpgradeInvalid)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	upgrade.mu.RLock()
	if upgrade.phase != TupleUpgradePhaseActive {
		phase := upgrade.phase
		upgrade.mu.RUnlock()
		return nil, fmt.Errorf("%w: cannot migrate in %s", ErrTupleUpgradePhase, phase)
	}
	startRow := upgrade.nextRow
	generation := upgrade.generation
	totalRows := upgrade.totalRows
	previous := upgrade.previous
	next := upgrade.next
	converter := upgrade.converter
	upgrade.mu.RUnlock()
	if startRow > totalRows || uint64(len(rows)) > totalRows-startRow {
		return nil, fmt.Errorf("%w: batch extends beyond %d rows", ErrTupleUpgradeInvalid, totalRows)
	}

	converted := make([]VersionedTuple, len(rows))
	var migrated uint64
	for index, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		convertedRow, wasMigrated, err := upgrade.convertForward(previous, next, converter, row)
		if err != nil {
			return nil, err
		}
		converted[index] = convertedRow
		if wasMigrated {
			migrated++
		}
	}
	if uint64(len(rows)) > ^uint64(0)-startRow {
		return nil, ErrTupleUpgradeInvalid
	}
	upgrade.mu.Lock()
	defer upgrade.mu.Unlock()
	if upgrade.phase != TupleUpgradePhaseActive || upgrade.generation != generation || upgrade.nextRow != startRow {
		return nil, ErrTupleUpgradeConflict
	}
	upgrade.nextRow += uint64(len(rows))
	upgrade.migrated += migrated
	upgrade.generation++
	return converted, nil
}

// BeginCutover freezes migration progress and enters the explicit cutover
// phase. Reads still accept both formats until CompleteCutover.
func (upgrade *OnlineTupleUpgrade) BeginCutover() error {
	if upgrade == nil {
		return ErrTupleUpgradeInvalid
	}
	upgrade.mu.Lock()
	defer upgrade.mu.Unlock()
	if upgrade.phase == TupleUpgradePhaseCutover || upgrade.phase == TupleUpgradePhaseComplete {
		return nil
	}
	if upgrade.phase != TupleUpgradePhaseActive {
		return fmt.Errorf("%w: cannot begin from %s", ErrTupleUpgradePhase, upgrade.phase)
	}
	if upgrade.nextRow < upgrade.totalRows {
		return fmt.Errorf("%w: migrated %d of %d rows", ErrTupleUpgradeIncomplete, upgrade.nextRow, upgrade.totalRows)
	}
	upgrade.phase = TupleUpgradePhaseCutover
	upgrade.generation++
	return nil
}

// CompleteCutover commits the next format as the only accepted read format.
func (upgrade *OnlineTupleUpgrade) CompleteCutover() error {
	if upgrade == nil {
		return ErrTupleUpgradeInvalid
	}
	upgrade.mu.Lock()
	defer upgrade.mu.Unlock()
	if upgrade.phase == TupleUpgradePhaseComplete {
		return nil
	}
	if upgrade.phase != TupleUpgradePhaseCutover {
		return fmt.Errorf("%w: cannot complete from %s", ErrTupleUpgradePhase, upgrade.phase)
	}
	upgrade.phase = TupleUpgradePhaseComplete
	upgrade.generation++
	return nil
}

// AbortCutover returns a prepared cutover to active migration.
func (upgrade *OnlineTupleUpgrade) AbortCutover() error {
	if upgrade == nil {
		return ErrTupleUpgradeInvalid
	}
	upgrade.mu.Lock()
	defer upgrade.mu.Unlock()
	if upgrade.phase == TupleUpgradePhaseActive {
		return nil
	}
	if upgrade.phase != TupleUpgradePhaseCutover {
		return fmt.Errorf("%w: cannot abort from %s", ErrTupleUpgradePhase, upgrade.phase)
	}
	upgrade.phase = TupleUpgradePhaseActive
	upgrade.generation++
	return nil
}

// Rollback makes the previous format the write/read format and enables the
// reverse converter for already-upgraded rows. A completed upgrade is kept
// terminal; callers that need another forward attempt create a new plan.
func (upgrade *OnlineTupleUpgrade) Rollback() error {
	if upgrade == nil {
		return ErrTupleUpgradeInvalid
	}
	upgrade.mu.Lock()
	defer upgrade.mu.Unlock()
	if upgrade.phase == TupleUpgradePhaseRolledBack {
		return nil
	}
	if upgrade.phase != TupleUpgradePhaseActive && upgrade.phase != TupleUpgradePhaseCutover {
		return fmt.Errorf("%w: cannot roll back from %s", ErrTupleUpgradePhase, upgrade.phase)
	}
	upgrade.phase = TupleUpgradePhaseRolledBack
	upgrade.generation++
	return nil
}

// Restore replaces coordinator progress after validating that the checkpoint
// belongs to this exact upgrade plan.
func (upgrade *OnlineTupleUpgrade) Restore(checkpoint TupleUpgradeCheckpoint) error {
	if upgrade == nil {
		return ErrTupleUpgradeInvalid
	}
	if err := validateTupleUpgradeCheckpoint(checkpoint); err != nil {
		return err
	}
	upgrade.mu.Lock()
	defer upgrade.mu.Unlock()
	if checkpoint.ID != upgrade.id || checkpoint.PreviousVersion != upgrade.previous.Version() || checkpoint.NextVersion != upgrade.next.Version() || checkpoint.TotalRows != upgrade.totalRows {
		return fmt.Errorf("%w: checkpoint does not match upgrade plan", ErrTupleUpgradeInvalid)
	}
	upgrade.nextRow = checkpoint.NextRow
	upgrade.migrated = checkpoint.MigratedRows
	upgrade.generation = checkpoint.Generation
	upgrade.phase = checkpoint.Phase
	return nil
}

func (upgrade *OnlineTupleUpgrade) checkpointLocked() TupleUpgradeCheckpoint {
	return TupleUpgradeCheckpoint{
		ID:              upgrade.id,
		PreviousVersion: upgrade.previous.Version(),
		NextVersion:     upgrade.next.Version(),
		TotalRows:       upgrade.totalRows,
		NextRow:         upgrade.nextRow,
		MigratedRows:    upgrade.migrated,
		Generation:      upgrade.generation,
		Phase:           upgrade.phase,
	}
}

func (upgrade *OnlineTupleUpgrade) readAtPhase(phase TupleUpgradePhase, previous, next TupleFormat, converter TupleUpgradeConverter, tuple VersionedTuple) (VersionedTuple, error) {
	switch phase {
	case TupleUpgradePhaseActive, TupleUpgradePhaseCutover:
		converted, _, err := upgrade.convertForward(previous, next, converter, tuple)
		return converted, err
	case TupleUpgradePhaseComplete:
		if tuple.Version() != next.Version() {
			return VersionedTuple{}, tupleUpgradeFormatMismatch(next.Version(), tuple.Version())
		}
		if err := tuple.Validate(next); err != nil {
			return VersionedTuple{}, err
		}
		return tuple, nil
	case TupleUpgradePhaseRolledBack:
		if tuple.Version() == previous.Version() {
			if err := tuple.Validate(previous); err != nil {
				return VersionedTuple{}, err
			}
			return tuple, nil
		}
		if tuple.Version() != next.Version() {
			return VersionedTuple{}, tupleUpgradeFormatMismatch(previous.Version(), tuple.Version())
		}
		if err := tuple.Validate(next); err != nil {
			return VersionedTuple{}, err
		}
		converted, err := converter.ToPrevious(tuple)
		if err != nil {
			return VersionedTuple{}, fmt.Errorf("%w: to previous: %v", ErrTupleUpgradeConversion, err)
		}
		if err := converted.Validate(previous); err != nil {
			return VersionedTuple{}, fmt.Errorf("%w: reverse result: %v", ErrTupleUpgradeConversion, err)
		}
		return converted, nil
	default:
		return VersionedTuple{}, ErrTupleUpgradeInvalid
	}
}

func (upgrade *OnlineTupleUpgrade) convertForward(previous, next TupleFormat, converter TupleUpgradeConverter, tuple VersionedTuple) (VersionedTuple, bool, error) {
	switch tuple.Version() {
	case next.Version():
		if err := tuple.Validate(next); err != nil {
			return VersionedTuple{}, false, err
		}
		return tuple, false, nil
	case previous.Version():
		if err := tuple.Validate(previous); err != nil {
			return VersionedTuple{}, false, err
		}
		converted, err := converter.ToNext(tuple)
		if err != nil {
			return VersionedTuple{}, false, fmt.Errorf("%w: to next: %v", ErrTupleUpgradeConversion, err)
		}
		if err := converted.Validate(next); err != nil {
			return VersionedTuple{}, false, fmt.Errorf("%w: forward result: %v", ErrTupleUpgradeConversion, err)
		}
		return converted, true, nil
	default:
		return VersionedTuple{}, false, tupleUpgradeFormatMismatch(previous.Version(), tuple.Version())
	}
}

func tupleUpgradeFormatMismatch(expected, got uint64) error {
	return fmt.Errorf("%w: expected %d or active version, got %d", ErrTupleUpgradeFormatMismatch, expected, got)
}

func validateTupleUpgradeCheckpoint(checkpoint TupleUpgradeCheckpoint) error {
	if strings.TrimSpace(checkpoint.ID) == "" || len(checkpoint.ID) > MaxTupleUpgradeIDBytes {
		return fmt.Errorf("%w: checkpoint ID is empty or too long", ErrTupleUpgradeInvalid)
	}
	if checkpoint.PreviousVersion == 0 || checkpoint.NextVersion == 0 || checkpoint.PreviousVersion == checkpoint.NextVersion {
		return fmt.Errorf("%w: checkpoint format versions are invalid", ErrTupleUpgradeInvalid)
	}
	if checkpoint.NextRow > checkpoint.TotalRows || checkpoint.MigratedRows > checkpoint.NextRow {
		return fmt.Errorf("%w: checkpoint progress is invalid", ErrTupleUpgradeInvalid)
	}
	switch checkpoint.Phase {
	case TupleUpgradePhaseActive, TupleUpgradePhaseCutover, TupleUpgradePhaseComplete, TupleUpgradePhaseRolledBack:
	default:
		return fmt.Errorf("%w: checkpoint phase %d is invalid", ErrTupleUpgradeInvalid, checkpoint.Phase)
	}
	if (checkpoint.Phase == TupleUpgradePhaseCutover || checkpoint.Phase == TupleUpgradePhaseComplete) && checkpoint.NextRow < checkpoint.TotalRows {
		return fmt.Errorf("%w: checkpoint cutover is incomplete", ErrTupleUpgradeInvalid)
	}
	return nil
}

// MarshalTupleUpgradeCheckpoint encodes a bounded HUP1 checkpoint with a
// CRC32C checksum and no trailing fields.
func MarshalTupleUpgradeCheckpoint(checkpoint TupleUpgradeCheckpoint) ([]byte, error) {
	if err := validateTupleUpgradeCheckpoint(checkpoint); err != nil {
		return nil, err
	}
	if len(checkpoint.ID) > 255 {
		return nil, fmt.Errorf("%w: checkpoint ID is too long for wire format", ErrTupleUpgradeInvalid)
	}
	const fixedBytes = 4 + 1 + 1 + 8*6 + 1 + 4
	encoded := make([]byte, fixedBytes+len(checkpoint.ID))
	copy(encoded[:4], tupleUpgradeCheckpointMagic[:])
	encoded[4] = tupleUpgradeCheckpointWireVersion
	encoded[5] = byte(len(checkpoint.ID))
	offset := 6
	copy(encoded[offset:], checkpoint.ID)
	offset += len(checkpoint.ID)
	for _, value := range []uint64{checkpoint.PreviousVersion, checkpoint.NextVersion, checkpoint.TotalRows, checkpoint.NextRow, checkpoint.MigratedRows, checkpoint.Generation} {
		binary.BigEndian.PutUint64(encoded[offset:], value)
		offset += 8
	}
	encoded[offset] = byte(checkpoint.Phase)
	offset++
	binary.BigEndian.PutUint32(encoded[offset:], crc32.Checksum(encoded[:offset], tupleUpgradeCheckpointCRCTable))
	if len(encoded) > MaxTupleUpgradeCheckpointBytes {
		return nil, ErrTupleUpgradeInvalid
	}
	return encoded, nil
}

// UnmarshalTupleUpgradeCheckpoint strictly decodes and validates one HUP1
// checkpoint before returning it to a caller for plan-bound Restore.
func UnmarshalTupleUpgradeCheckpoint(encoded []byte) (TupleUpgradeCheckpoint, error) {
	const fixedBytes = 4 + 1 + 1 + 8*6 + 1 + 4
	if len(encoded) < fixedBytes || len(encoded) > MaxTupleUpgradeCheckpointBytes {
		return TupleUpgradeCheckpoint{}, ErrTupleUpgradeCheckpointWire
	}
	if string(encoded[:4]) != string(tupleUpgradeCheckpointMagic[:]) || encoded[4] != tupleUpgradeCheckpointWireVersion {
		return TupleUpgradeCheckpoint{}, ErrTupleUpgradeCheckpointWire
	}
	idLength := int(encoded[5])
	if idLength == 0 || idLength > MaxTupleUpgradeIDBytes || len(encoded) != fixedBytes+idLength {
		return TupleUpgradeCheckpoint{}, ErrTupleUpgradeCheckpointWire
	}
	checksumOffset := len(encoded) - 4
	want := binary.BigEndian.Uint32(encoded[checksumOffset:])
	got := crc32.Checksum(encoded[:checksumOffset], tupleUpgradeCheckpointCRCTable)
	if want != got {
		return TupleUpgradeCheckpoint{}, ErrTupleUpgradeCheckpointWire
	}
	offset := 6
	checkpoint := TupleUpgradeCheckpoint{ID: string(encoded[offset : offset+idLength])}
	offset += idLength
	values := []*uint64{
		&checkpoint.PreviousVersion,
		&checkpoint.NextVersion,
		&checkpoint.TotalRows,
		&checkpoint.NextRow,
		&checkpoint.MigratedRows,
		&checkpoint.Generation,
	}
	for _, value := range values {
		*value = binary.BigEndian.Uint64(encoded[offset:])
		offset += 8
	}
	checkpoint.Phase = TupleUpgradePhase(encoded[offset])
	if err := validateTupleUpgradeCheckpoint(checkpoint); err != nil {
		return TupleUpgradeCheckpoint{}, fmt.Errorf("%w: %v", ErrTupleUpgradeCheckpointWire, err)
	}
	return checkpoint, nil
}
