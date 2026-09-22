package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	durableSequenceFormatVersion byte = 1
	durableSequenceFrameSize          = 20
)

var durableSequenceMagic = [4]byte{'H', 'S', 'Q', '1'}
var durableSequenceCRCTable = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrDurableSequenceNil       = errors.New("hatDataStructure: durable sequence is nil")
	ErrDurableSequencePath      = errors.New("hatDataStructure: durable sequence has no file path")
	ErrDurableSequenceClosed    = errors.New("hatDataStructure: durable sequence is closed")
	ErrDurableSequenceExhausted = errors.New("hatDataStructure: durable sequence is exhausted")
	ErrDurableSequenceFormat    = errors.New("hatDataStructure: durable sequence format is invalid")
	ErrDurableSequenceChecksum  = errors.New("hatDataStructure: durable sequence checksum mismatch")
)

// DurableSequenceOptions configures a monotonic sequence. An empty Path keeps
// the sequence in memory. With a path, every successful allocation is synced
// before the value is returned, so a restarted process never reuses a value.
type DurableSequenceOptions struct {
	Path    string
	Initial uint64
}

// DurableSequenceSnapshot is the detached current value of a sequence.
type DurableSequenceSnapshot struct {
	Current uint64
}

// DurableSequence allocates monotonically increasing values. It is safe for
// concurrent callers. A durable allocation may leave a hole if the process
// crashes after the file is synced but before the caller receives the value;
// the stronger and useful guarantee is no duplicate or regressing value.
type DurableSequence struct {
	mu          sync.Mutex
	path        string
	file        *os.File
	current     uint64
	memory      atomic.Uint64
	durableSlot int
	closed      bool
}

// NewDurableSequence creates an in-memory sequence or opens an existing file.
// Initial is used only when Path does not contain a valid frame.
func NewDurableSequence(options DurableSequenceOptions) (*DurableSequence, error) {
	if options.Path == "" {
		sequence := &DurableSequence{}
		sequence.memory.Store(options.Initial)
		return sequence, nil
	}
	path := filepath.Clean(options.Path)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("hatDataStructure: open durable sequence: %w", err)
	}
	closeWithError := func(operation string, operationErr error) (*DurableSequence, error) {
		_ = file.Close()
		return nil, fmt.Errorf("hatDataStructure: %s durable sequence: %w", operation, operationErr)
	}
	if err := file.Chmod(0o600); err != nil {
		return closeWithError("chmod", err)
	}
	current, slot, err := readDurableSequenceFile(file)
	if err != nil {
		return closeWithError("read", err)
	}
	if err := syncDurableSequenceDirectory(filepath.Dir(path)); err != nil {
		return closeWithError("sync directory", err)
	}
	if slot < 0 {
		current = options.Initial
	}
	return &DurableSequence{
		path:        path,
		file:        file,
		current:     current,
		durableSlot: slot,
	}, nil
}

// NewDurableSequenceFromSnapshot creates an in-memory sequence from a
// validated snapshot.
func NewDurableSequenceFromSnapshot(snapshot DurableSequenceSnapshot) *DurableSequence {
	sequence := &DurableSequence{}
	sequence.memory.Store(snapshot.Current)
	return sequence
}

// Current returns the last value allocated by the sequence.
func (sequence *DurableSequence) Current() uint64 {
	if sequence == nil {
		return 0
	}
	if sequence.path == "" {
		return sequence.memory.Load()
	}
	sequence.mu.Lock()
	defer sequence.mu.Unlock()
	return sequence.current
}

// Snapshot returns the current sequence value without exposing mutable state.
func (sequence *DurableSequence) Snapshot() DurableSequenceSnapshot {
	return DurableSequenceSnapshot{Current: sequence.Current()}
}

// Allocate returns the next value. A path-backed sequence persists and syncs
// the candidate before making it visible to the caller.
func (sequence *DurableSequence) Allocate() (uint64, error) {
	if sequence == nil {
		return 0, ErrDurableSequenceNil
	}
	if sequence.path == "" {
		return sequence.allocateMemory()
	}
	sequence.mu.Lock()
	defer sequence.mu.Unlock()
	if sequence.closed {
		return 0, ErrDurableSequenceClosed
	}
	if sequence.current == ^uint64(0) {
		return 0, ErrDurableSequenceExhausted
	}
	next := sequence.current + 1
	if err := sequence.persistCurrentLocked(next); err != nil {
		return 0, err
	}
	sequence.current = next
	return next, nil
}

func (sequence *DurableSequence) allocateMemory() (uint64, error) {
	for {
		current := sequence.memory.Load()
		if current == ^uint64(0) {
			return 0, ErrDurableSequenceExhausted
		}
		if sequence.memory.CompareAndSwap(current, current+1) {
			return current + 1, nil
		}
	}
}

// MarshalBinary returns a fixed-width CRC-protected sequence frame.
func (sequence *DurableSequence) MarshalBinary() ([]byte, error) {
	if sequence == nil {
		return nil, ErrDurableSequenceNil
	}
	return encodeDurableSequence(sequence.Current()), nil
}

// UnmarshalDurableSequence decodes a sequence frame into an in-memory
// sequence.
func UnmarshalDurableSequence(data []byte) (*DurableSequence, error) {
	current, err := decodeDurableSequence(data)
	if err != nil {
		return nil, err
	}
	return NewDurableSequenceFromSnapshot(DurableSequenceSnapshot{Current: current}), nil
}

// Save syncs the current value to the configured path. Allocate already saves
// path-backed sequences; Save is useful for an explicit checkpoint.
func (sequence *DurableSequence) Save() error {
	if sequence == nil {
		return ErrDurableSequenceNil
	}
	if sequence.path == "" {
		return ErrDurableSequencePath
	}
	sequence.mu.Lock()
	defer sequence.mu.Unlock()
	if sequence.closed {
		return ErrDurableSequenceClosed
	}
	return sequence.persistCurrentLocked(sequence.current)
}

// Close releases a path-backed sequence file. In-memory sequences do not
// hold resources and Close is a no-op.
func (sequence *DurableSequence) Close() error {
	if sequence == nil {
		return ErrDurableSequenceNil
	}
	if sequence.path == "" {
		return nil
	}
	sequence.mu.Lock()
	defer sequence.mu.Unlock()
	if sequence.closed {
		return nil
	}
	sequence.closed = true
	if sequence.file == nil {
		return nil
	}
	err := sequence.file.Close()
	sequence.file = nil
	return err
}

func (sequence *DurableSequence) persistCurrentLocked(current uint64) error {
	if sequence.file == nil || sequence.closed {
		return ErrDurableSequenceClosed
	}
	slot := 0
	if sequence.durableSlot >= 0 {
		slot = 1 - sequence.durableSlot
	}
	payload := encodeDurableSequence(current)
	written, err := sequence.file.WriteAt(payload, int64(slot*durableSequenceFrameSize))
	if err != nil {
		return fmt.Errorf("hatDataStructure: write durable sequence: %w", err)
	}
	if written != len(payload) {
		return fmt.Errorf("hatDataStructure: write durable sequence: %w", io.ErrShortWrite)
	}
	if err := sequence.file.Sync(); err != nil {
		return fmt.Errorf("hatDataStructure: sync durable sequence: %w", err)
	}
	sequence.durableSlot = slot
	return nil
}

func encodeDurableSequence(current uint64) []byte {
	data := make([]byte, durableSequenceFrameSize)
	copy(data[:4], durableSequenceMagic[:])
	data[4] = durableSequenceFormatVersion
	binary.LittleEndian.PutUint64(data[8:16], current)
	binary.LittleEndian.PutUint32(data[16:20], crc32.Checksum(data[:16], durableSequenceCRCTable))
	return data
}

func decodeDurableSequence(data []byte) (uint64, error) {
	if len(data) != durableSequenceFrameSize {
		return 0, ErrDurableSequenceFormat
	}
	if string(data[:4]) != string(durableSequenceMagic[:]) || data[4] != durableSequenceFormatVersion || data[5] != 0 || data[6] != 0 || data[7] != 0 {
		return 0, ErrDurableSequenceFormat
	}
	want := binary.LittleEndian.Uint32(data[16:20])
	if got := crc32.Checksum(data[:16], durableSequenceCRCTable); got != want {
		return 0, ErrDurableSequenceChecksum
	}
	return binary.LittleEndian.Uint64(data[8:16]), nil
}

func readDurableSequenceFile(file *os.File) (uint64, int, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, -1, err
	}
	size := info.Size()
	if size == 0 {
		return 0, -1, nil
	}
	if size != durableSequenceFrameSize && size != 2*durableSequenceFrameSize {
		return 0, -1, ErrDurableSequenceFormat
	}
	current := uint64(0)
	validSlot := -1
	var firstErr error
	for slot := 0; int64(slot+1)*durableSequenceFrameSize <= size; slot++ {
		data := make([]byte, durableSequenceFrameSize)
		read, readErr := file.ReadAt(data, int64(slot*durableSequenceFrameSize))
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return 0, -1, readErr
		}
		if read != durableSequenceFrameSize {
			if firstErr == nil {
				firstErr = io.ErrUnexpectedEOF
			}
			continue
		}
		value, decodeErr := decodeDurableSequence(data)
		if decodeErr != nil {
			if firstErr == nil {
				firstErr = decodeErr
			}
			continue
		}
		if validSlot < 0 || value >= current {
			current = value
			validSlot = slot
		}
	}
	if validSlot < 0 {
		if firstErr != nil {
			return 0, -1, firstErr
		}
		return 0, -1, ErrDurableSequenceFormat
	}
	return current, validSlot, nil
}

func syncDurableSequenceDirectory(directory string) error {
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	if err := directoryFile.Sync(); err != nil {
		directoryFile.Close()
		return err
	}
	return directoryFile.Close()
}
