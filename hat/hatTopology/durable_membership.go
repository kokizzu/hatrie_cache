package hatTopology

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	// DefaultDurableMembershipMaxMembers bounds the retained membership set when
	// callers do not provide a limit.
	DefaultDurableMembershipMaxMembers = 256
	MaxDurableMembershipMembers        = 4096
	MaxDurableMembershipNameBytes      = 256
	MaxDurableMembershipAddressBytes   = 1024
	MaxDurableMembershipBytes          = 1 << 20

	durableMembershipFormatVersion = 1
	durableMembershipHeaderBytes   = 4 + 1 + 8 + 4
	durableMembershipChecksumBytes = 4
)

var (
	ErrDurableMembershipInvalid         = errors.New("hatTopology: durable membership is invalid")
	ErrDurableMembershipCorrupt         = errors.New("hatTopology: durable membership is corrupt")
	ErrDurableMembershipStoreRequired   = errors.New("hatTopology: durable membership store is required")
	ErrDurableMembershipDuplicate       = errors.New("hatTopology: durable membership name or address is already active")
	ErrDurableMembershipNotFound        = errors.New("hatTopology: durable membership member is not found")
	ErrDurableMembershipStaleGeneration = errors.New("hatTopology: durable membership generation is stale")
	ErrDurableMembershipLimit           = errors.New("hatTopology: durable membership member limit reached")
)

var durableMembershipCRCTable = crc32.MakeTable(crc32.Castagnoli)

const durableMembershipMagic = "hmb1"

// MembershipState describes the durable lifecycle state of one member.
type MembershipState uint8

const (
	MembershipStateInvalid MembershipState = iota
	MembershipStateActive
	MembershipStateRemoved
)

// ClusterMember is one durable member record. Removed records are retained as
// tombstones so an old generation cannot silently rejoin.
type ClusterMember struct {
	Name       string
	Address    string
	Generation uint64
	State      MembershipState
}

// MembershipSnapshot is the complete versioned membership image.
type MembershipSnapshot struct {
	Revision uint64
	Members  []ClusterMember
}

// DurableMembershipOptions bounds one registry. A zero MaxMembers selects the
// sane default; negative or excessive limits are rejected.
type DurableMembershipOptions struct {
	MaxMembers int
}

// MembershipStore is the persistence boundary for a membership registry.
// Save must replace the supplied image atomically from the registry's point of
// view. FileMembershipStore provides that contract for local files.
type MembershipStore interface {
	Load() ([]byte, error)
	Save(payload []byte) error
}

// DurableMembershipRegistry owns serialized join/leave transitions and
// generation fencing. The registry is safe for concurrent callers.
type DurableMembershipRegistry struct {
	mu         sync.RWMutex
	store      MembershipStore
	maxMembers int
	revision   uint64
	members    map[string]ClusterMember
}

// FileMembershipStore persists membership images using a private temporary
// file, fsync, rename, and directory fsync.
type FileMembershipStore struct {
	path string
}

// NewFileMembershipStore creates a local file-backed membership store.
func NewFileMembershipStore(path string) (*FileMembershipStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrDurableMembershipInvalid
	}
	return &FileMembershipStore{path: filepath.Clean(path)}, nil
}

// OpenDurableMembershipRegistry opens a file-backed registry and restores its
// last complete image. A missing file starts an empty registry.
func OpenDurableMembershipRegistry(path string, options DurableMembershipOptions) (*DurableMembershipRegistry, error) {
	store, err := NewFileMembershipStore(path)
	if err != nil {
		return nil, err
	}
	return NewDurableMembershipRegistry(store, options)
}

// NewDurableMembershipRegistry restores a registry from store. A missing or
// empty store is treated as a new empty membership set.
func NewDurableMembershipRegistry(store MembershipStore, options DurableMembershipOptions) (*DurableMembershipRegistry, error) {
	if store == nil {
		return nil, ErrDurableMembershipStoreRequired
	}
	maxMembers := options.MaxMembers
	if maxMembers == 0 {
		maxMembers = DefaultDurableMembershipMaxMembers
	}
	if maxMembers < 1 || maxMembers > MaxDurableMembershipMembers {
		return nil, ErrDurableMembershipInvalid
	}
	registry := &DurableMembershipRegistry{
		store:      store,
		maxMembers: maxMembers,
		members:    make(map[string]ClusterMember),
	}
	payload, err := store.Load()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return registry, nil
		}
		return nil, err
	}
	if len(payload) == 0 {
		return registry, nil
	}
	snapshot, err := DecodeMembershipSnapshot(payload)
	if err != nil {
		return nil, err
	}
	if len(snapshot.Members) > maxMembers {
		return nil, ErrDurableMembershipLimit
	}
	for _, member := range snapshot.Members {
		registry.members[member.Name] = member
	}
	registry.revision = snapshot.Revision
	return registry, nil
}

// Join activates a member and advances its generation. Rejoining a removed
// name is allowed only with a new generation and is persisted before return.
func (registry *DurableMembershipRegistry) Join(name, address string) (ClusterMember, error) {
	if registry == nil {
		return ClusterMember{}, ErrDurableMembershipInvalid
	}
	name, err := normalizeDurableMembershipName(name, MaxDurableMembershipNameBytes)
	if err != nil {
		return ClusterMember{}, err
	}
	address, err = normalizeDurableMembershipName(address, MaxDurableMembershipAddressBytes)
	if err != nil {
		return ClusterMember{}, err
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	previous, existed := registry.members[name]
	if existed && previous.State == MembershipStateActive {
		return ClusterMember{}, ErrDurableMembershipDuplicate
	}
	for _, member := range registry.members {
		if member.State == MembershipStateActive && member.Address == address && member.Name != name {
			return ClusterMember{}, ErrDurableMembershipDuplicate
		}
	}
	if !existed && len(registry.members) >= registry.maxMembers {
		return ClusterMember{}, ErrDurableMembershipLimit
	}
	generation := uint64(1)
	if existed {
		if previous.Generation == ^uint64(0) {
			return ClusterMember{}, ErrDurableMembershipInvalid
		}
		generation = previous.Generation + 1
	}
	member := ClusterMember{Name: name, Address: address, Generation: generation, State: MembershipStateActive}
	oldRevision := registry.revision
	registry.members[name] = member
	registry.revision++
	if err := registry.persistLocked(); err != nil {
		if existed {
			registry.members[name] = previous
		} else {
			delete(registry.members, name)
		}
		registry.revision = oldRevision
		return ClusterMember{}, err
	}
	return member, nil
}

// Leave removes a member only when the caller presents its current generation.
// The tombstone is retained and persisted for stale-node fencing.
func (registry *DurableMembershipRegistry) Leave(name string, generation uint64) error {
	if registry == nil {
		return ErrDurableMembershipInvalid
	}
	name, err := normalizeDurableMembershipName(name, MaxDurableMembershipNameBytes)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	member, ok := registry.members[name]
	if !ok {
		return ErrDurableMembershipNotFound
	}
	if member.State != MembershipStateActive || member.Generation != generation {
		return ErrDurableMembershipStaleGeneration
	}
	previous := member
	oldRevision := registry.revision
	member.State = MembershipStateRemoved
	registry.members[name] = member
	registry.revision++
	if err := registry.persistLocked(); err != nil {
		registry.members[name] = previous
		registry.revision = oldRevision
		return err
	}
	return nil
}

// Member returns a copy of one member record.
func (registry *DurableMembershipRegistry) Member(name string) (ClusterMember, bool) {
	if registry == nil {
		return ClusterMember{}, false
	}
	name, err := normalizeDurableMembershipName(name, MaxDurableMembershipNameBytes)
	if err != nil {
		return ClusterMember{}, false
	}
	registry.mu.RLock()
	member, ok := registry.members[name]
	registry.mu.RUnlock()
	return member, ok
}

// Snapshot returns all active members and retained tombstones in name order.
func (registry *DurableMembershipRegistry) Snapshot() MembershipSnapshot {
	if registry == nil {
		return MembershipSnapshot{}
	}
	registry.mu.RLock()
	snapshot := registry.snapshotLocked()
	registry.mu.RUnlock()
	return snapshot
}

// ActiveMembers returns only currently active members in name order.
func (registry *DurableMembershipRegistry) ActiveMembers() []ClusterMember {
	snapshot := registry.Snapshot()
	active := make([]ClusterMember, 0, len(snapshot.Members))
	for _, member := range snapshot.Members {
		if member.State == MembershipStateActive {
			active = append(active, member)
		}
	}
	return active
}

func (registry *DurableMembershipRegistry) snapshotLocked() MembershipSnapshot {
	members := make([]ClusterMember, 0, len(registry.members))
	for _, member := range registry.members {
		members = append(members, member)
	}
	sort.Slice(members, func(left, right int) bool { return members[left].Name < members[right].Name })
	return MembershipSnapshot{Revision: registry.revision, Members: members}
}

func (registry *DurableMembershipRegistry) persistLocked() error {
	payload, err := EncodeMembershipSnapshot(registry.snapshotLocked())
	if err != nil {
		return err
	}
	return registry.store.Save(payload)
}

// EncodeMembershipSnapshot creates a bounded deterministic HMB1 image with a
// CRC32C checksum over the header and records.
func EncodeMembershipSnapshot(snapshot MembershipSnapshot) ([]byte, error) {
	if len(snapshot.Members) > MaxDurableMembershipMembers {
		return nil, ErrDurableMembershipLimit
	}
	members := append([]ClusterMember(nil), snapshot.Members...)
	if err := validateAndNormalizeMemberships(members); err != nil {
		return nil, err
	}
	sort.Slice(members, func(left, right int) bool { return members[left].Name < members[right].Name })
	capacity := durableMembershipHeaderBytes + durableMembershipChecksumBytes
	for _, member := range members {
		capacity += 2 + 2 + 1 + 8 + len(member.Name) + len(member.Address)
	}
	if capacity > MaxDurableMembershipBytes {
		return nil, ErrDurableMembershipLimit
	}
	payload := make([]byte, 0, capacity)
	payload = append(payload, durableMembershipMagic...)
	payload = append(payload, durableMembershipFormatVersion)
	var fixed [12]byte
	binary.BigEndian.PutUint64(fixed[:8], snapshot.Revision)
	binary.BigEndian.PutUint32(fixed[8:], uint32(len(members)))
	payload = append(payload, fixed[:]...)
	for _, member := range members {
		var record [13]byte
		binary.BigEndian.PutUint16(record[:2], uint16(len(member.Name)))
		binary.BigEndian.PutUint16(record[2:4], uint16(len(member.Address)))
		record[4] = byte(member.State)
		binary.BigEndian.PutUint64(record[5:], member.Generation)
		payload = append(payload, record[:]...)
		payload = append(payload, member.Name...)
		payload = append(payload, member.Address...)
	}
	checksum := crc32.Checksum(payload, durableMembershipCRCTable)
	var checksumBytes [4]byte
	binary.BigEndian.PutUint32(checksumBytes[:], checksum)
	payload = append(payload, checksumBytes[:]...)
	return payload, nil
}

// DecodeMembershipSnapshot verifies and decodes one bounded HMB1 image.
func DecodeMembershipSnapshot(payload []byte) (MembershipSnapshot, error) {
	if len(payload) < durableMembershipHeaderBytes+durableMembershipChecksumBytes || len(payload) > MaxDurableMembershipBytes {
		return MembershipSnapshot{}, ErrDurableMembershipCorrupt
	}
	if string(payload[:4]) != durableMembershipMagic || payload[4] != durableMembershipFormatVersion {
		return MembershipSnapshot{}, ErrDurableMembershipCorrupt
	}
	storedChecksum := binary.BigEndian.Uint32(payload[len(payload)-durableMembershipChecksumBytes:])
	actualChecksum := crc32.Checksum(payload[:len(payload)-durableMembershipChecksumBytes], durableMembershipCRCTable)
	if storedChecksum != actualChecksum {
		return MembershipSnapshot{}, ErrDurableMembershipCorrupt
	}
	snapshot := MembershipSnapshot{
		Revision: binary.BigEndian.Uint64(payload[5:13]),
	}
	count := binary.BigEndian.Uint32(payload[13:17])
	if count > MaxDurableMembershipMembers || count > 0 && snapshot.Revision == 0 {
		return MembershipSnapshot{}, ErrDurableMembershipCorrupt
	}
	snapshot.Members = make([]ClusterMember, 0, count)
	seenNames := make(map[string]struct{}, count)
	activeAddresses := make(map[string]struct{}, count)
	offset := durableMembershipHeaderBytes
	end := len(payload) - durableMembershipChecksumBytes
	for index := uint32(0); index < count; index++ {
		if offset+13 > end {
			return MembershipSnapshot{}, ErrDurableMembershipCorrupt
		}
		nameLength := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		addressLength := int(binary.BigEndian.Uint16(payload[offset+2 : offset+4]))
		state := MembershipState(payload[offset+4])
		generation := binary.BigEndian.Uint64(payload[offset+5 : offset+13])
		offset += 13
		if nameLength == 0 || nameLength > MaxDurableMembershipNameBytes || addressLength == 0 || addressLength > MaxDurableMembershipAddressBytes || offset+nameLength+addressLength > end {
			return MembershipSnapshot{}, ErrDurableMembershipCorrupt
		}
		name := string(payload[offset : offset+nameLength])
		offset += nameLength
		address := string(payload[offset : offset+addressLength])
		offset += addressLength
		member := ClusterMember{Name: name, Address: address, Generation: generation, State: state}
		if err := validateDecodedMember(member, seenNames, activeAddresses); err != nil {
			return MembershipSnapshot{}, ErrDurableMembershipCorrupt
		}
		seenNames[name] = struct{}{}
		if state == MembershipStateActive {
			activeAddresses[address] = struct{}{}
		}
		snapshot.Members = append(snapshot.Members, member)
	}
	if offset != end {
		return MembershipSnapshot{}, ErrDurableMembershipCorrupt
	}
	return snapshot, nil
}

func validateAndNormalizeMemberships(members []ClusterMember) error {
	seenNames := make(map[string]struct{}, len(members))
	activeAddresses := make(map[string]struct{}, len(members))
	for index := range members {
		name, err := normalizeDurableMembershipName(members[index].Name, MaxDurableMembershipNameBytes)
		if err != nil {
			return err
		}
		address, err := normalizeDurableMembershipName(members[index].Address, MaxDurableMembershipAddressBytes)
		if err != nil {
			return err
		}
		members[index].Name = name
		members[index].Address = address
		if members[index].Generation == 0 || !validMembershipState(members[index].State) {
			return ErrDurableMembershipInvalid
		}
		if _, exists := seenNames[name]; exists {
			return ErrDurableMembershipDuplicate
		}
		seenNames[name] = struct{}{}
		if members[index].State == MembershipStateActive {
			if _, exists := activeAddresses[address]; exists {
				return ErrDurableMembershipDuplicate
			}
			activeAddresses[address] = struct{}{}
		}
	}
	return nil
}

func validateDecodedMember(member ClusterMember, seenNames, activeAddresses map[string]struct{}) error {
	name, err := normalizeDurableMembershipName(member.Name, MaxDurableMembershipNameBytes)
	if err != nil {
		return err
	}
	if name != member.Name {
		return ErrDurableMembershipInvalid
	}
	address, err := normalizeDurableMembershipName(member.Address, MaxDurableMembershipAddressBytes)
	if err != nil {
		return err
	}
	if address != member.Address {
		return ErrDurableMembershipInvalid
	}
	if member.Generation == 0 || !validMembershipState(member.State) {
		return ErrDurableMembershipInvalid
	}
	if _, exists := seenNames[member.Name]; exists {
		return ErrDurableMembershipDuplicate
	}
	if member.State == MembershipStateActive {
		if _, exists := activeAddresses[member.Address]; exists {
			return ErrDurableMembershipDuplicate
		}
	}
	return nil
}

func validMembershipState(state MembershipState) bool {
	return state == MembershipStateActive || state == MembershipStateRemoved
}

func normalizeDurableMembershipName(value string, maxBytes int) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxBytes || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return "", ErrDurableMembershipInvalid
	}
	return value, nil
}

// Load reads a bounded membership image from disk.
func (store *FileMembershipStore) Load() ([]byte, error) {
	if store == nil || store.path == "" {
		return nil, ErrDurableMembershipInvalid
	}
	file, err := os.Open(store.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, MaxDurableMembershipBytes+1))
	if err != nil {
		return nil, err
	}
	if len(payload) > MaxDurableMembershipBytes {
		return nil, ErrDurableMembershipLimit
	}
	return payload, nil
}

// Save atomically replaces the membership image and keeps the file private.
func (store *FileMembershipStore) Save(payload []byte) error {
	if store == nil || store.path == "" || len(payload) > MaxDurableMembershipBytes {
		return ErrDurableMembershipInvalid
	}
	directory := filepath.Dir(store.path)
	temporary, err := os.CreateTemp(directory, ".hatrie-membership-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	renamed := false
	defer func() {
		_ = temporary.Close()
		if !renamed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return err
	}
	renamed = true
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer directoryFile.Close()
	if err := directoryFile.Sync(); err != nil {
		return err
	}
	return nil
}

// String makes state values safe for diagnostics.
func (state MembershipState) String() string {
	switch state {
	case MembershipStateActive:
		return "active"
	case MembershipStateRemoved:
		return "removed"
	default:
		return fmt.Sprintf("unknown(%d)", state)
	}
}
