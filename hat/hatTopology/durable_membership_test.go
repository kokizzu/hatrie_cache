package hatTopology

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type memoryMembershipStore struct {
	mu      sync.Mutex
	payload []byte
	loadErr error
	saveErr error
}

func (store *memoryMembershipStore) Load() ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.loadErr != nil {
		return nil, store.loadErr
	}
	if store.payload == nil {
		return nil, os.ErrNotExist
	}
	return append([]byte(nil), store.payload...), nil
}

func (store *memoryMembershipStore) Save(payload []byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saveErr != nil {
		return store.saveErr
	}
	store.payload = append([]byte(nil), payload...)
	return nil
}

func TestDurableMembershipJoinLeavePersistsAndFences(t *testing.T) {
	store := &memoryMembershipStore{}
	registry, err := NewDurableMembershipRegistry(store, DurableMembershipOptions{MaxMembers: 4})
	if err != nil {
		t.Fatalf("NewDurableMembershipRegistry() error = %v", err)
	}
	member, err := registry.Join(" node-a ", " 127.0.0.1:9001 ")
	if err != nil {
		t.Fatalf("Join() error = %v", err)
	}
	if member.Name != "node-a" || member.Address != "127.0.0.1:9001" || member.Generation != 1 || member.State != MembershipStateActive {
		t.Fatalf("Join() member = %#v, want normalized active generation 1", member)
	}
	if _, err := registry.Join("node-a", "127.0.0.1:9002"); !errors.Is(err, ErrDurableMembershipDuplicate) {
		t.Fatalf("duplicate name error = %v, want ErrDurableMembershipDuplicate", err)
	}
	if _, err := registry.Join("node-b", "127.0.0.1:9001"); !errors.Is(err, ErrDurableMembershipDuplicate) {
		t.Fatalf("duplicate address error = %v, want ErrDurableMembershipDuplicate", err)
	}
	if err := registry.Leave("node-a", 0); !errors.Is(err, ErrDurableMembershipStaleGeneration) {
		t.Fatalf("stale Leave() error = %v, want ErrDurableMembershipStaleGeneration", err)
	}
	if err := registry.Leave("node-a", 1); err != nil {
		t.Fatalf("Leave() error = %v", err)
	}
	removed, ok := registry.Member("node-a")
	if !ok || removed.State != MembershipStateRemoved || removed.Generation != 1 {
		t.Fatalf("removed member = %#v/%v, want retained generation-1 tombstone", removed, ok)
	}

	restored, err := NewDurableMembershipRegistry(store, DurableMembershipOptions{MaxMembers: 4})
	if err != nil {
		t.Fatalf("restore error = %v", err)
	}
	if got, ok := restored.Member("node-a"); !ok || got.State != MembershipStateRemoved {
		t.Fatalf("restored tombstone = %#v/%v, want removed member", got, ok)
	}
	member, err = restored.Join("node-a", "127.0.0.1:9002")
	if err != nil {
		t.Fatalf("rejoin error = %v", err)
	}
	if member.Generation != 2 || member.State != MembershipStateActive {
		t.Fatalf("rejoin member = %#v, want generation 2 active", member)
	}
	if err := restored.Leave("node-a", 1); !errors.Is(err, ErrDurableMembershipStaleGeneration) {
		t.Fatalf("old generation Leave() error = %v, want stale generation", err)
	}
	if got := restored.Snapshot(); got.Revision != 3 || len(got.Members) != 1 || got.Members[0].State != MembershipStateActive {
		t.Fatalf("snapshot after rejoin = %#v, want revision 3 and one active member", got)
	}
}

func TestDurableMembershipEncodingRejectsCorruptionAndStoreFailure(t *testing.T) {
	snapshot := MembershipSnapshot{
		Revision: 7,
		Members:  []ClusterMember{{Name: "node-a", Address: "127.0.0.1:9001", Generation: 4, State: MembershipStateActive}},
	}
	payload, err := EncodeMembershipSnapshot(snapshot)
	if err != nil {
		t.Fatalf("EncodeMembershipSnapshot() error = %v", err)
	}
	decoded, err := DecodeMembershipSnapshot(payload)
	if err != nil || len(decoded.Members) != 1 || decoded.Members[0] != snapshot.Members[0] || decoded.Revision != snapshot.Revision {
		t.Fatalf("DecodeMembershipSnapshot() = %#v/%v, want %#v", decoded, err, snapshot)
	}
	corrupt := append([]byte(nil), payload...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := DecodeMembershipSnapshot(corrupt); !errors.Is(err, ErrDurableMembershipCorrupt) {
		t.Fatalf("corrupt decode error = %v, want ErrDurableMembershipCorrupt", err)
	}

	store := &memoryMembershipStore{saveErr: errors.New("save failed")}
	registry, err := NewDurableMembershipRegistry(store, DurableMembershipOptions{})
	if err != nil {
		t.Fatalf("NewDurableMembershipRegistry() error = %v", err)
	}
	if _, err := registry.Join("node-a", "127.0.0.1:9001"); err == nil || !strings.Contains(err.Error(), "save failed") {
		t.Fatalf("failed Join() error = %v, want save failure", err)
	}
	if _, ok := registry.Member("node-a"); ok {
		t.Fatal("failed Join() mutated in-memory membership")
	}
}

func TestDurableMembershipFileRoundTripAndBounds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.bin")
	store, err := NewFileMembershipStore(path)
	if err != nil {
		t.Fatalf("NewFileMembershipStore() error = %v", err)
	}
	registry, err := NewDurableMembershipRegistry(store, DurableMembershipOptions{MaxMembers: 1})
	if err != nil {
		t.Fatalf("NewDurableMembershipRegistry() error = %v", err)
	}
	if _, err := registry.Join("node-a", "127.0.0.1:9001"); err != nil {
		t.Fatalf("file Join() error = %v", err)
	}
	if _, err := registry.Join("node-b", "127.0.0.1:9002"); !errors.Is(err, ErrDurableMembershipLimit) {
		t.Fatalf("limit error = %v, want ErrDurableMembershipLimit", err)
	}
	restored, err := NewDurableMembershipRegistry(store, DurableMembershipOptions{MaxMembers: 1})
	if err != nil {
		t.Fatalf("file restore error = %v", err)
	}
	if member, ok := restored.Member("node-a"); !ok || member.Address != "127.0.0.1:9001" {
		t.Fatalf("file restored member = %#v/%v", member, ok)
	}

	for _, input := range [][2]string{{"", "address"}, {"name", ""}, {"bad\x00name", "address"}, {strings.Repeat("n", MaxDurableMembershipNameBytes+1), "address"}} {
		if _, err := restored.Join(input[0], input[1]); !errors.Is(err, ErrDurableMembershipInvalid) {
			t.Fatalf("Join(%q,%q) error = %v, want ErrDurableMembershipInvalid", input[0], input[1], err)
		}
	}
	if _, err := NewDurableMembershipRegistry(store, DurableMembershipOptions{MaxMembers: -1}); !errors.Is(err, ErrDurableMembershipInvalid) {
		t.Fatalf("negative MaxMembers error = %v, want ErrDurableMembershipInvalid", err)
	}
}

func TestDurableMembershipSnapshotIsIndependentAndSorted(t *testing.T) {
	store := &memoryMembershipStore{}
	registry, err := NewDurableMembershipRegistry(store, DurableMembershipOptions{MaxMembers: 4})
	if err != nil {
		t.Fatalf("NewDurableMembershipRegistry() error = %v", err)
	}
	for _, name := range []string{"node-c", "node-a", "node-b"} {
		if _, err := registry.Join(name, name+":9001"); err != nil {
			t.Fatalf("Join(%q) error = %v", name, err)
		}
	}
	first := registry.Snapshot()
	if got := []string{first.Members[0].Name, first.Members[1].Name, first.Members[2].Name}; !bytes.Equal([]byte(strings.Join(got, ",")), []byte("node-a,node-b,node-c")) {
		t.Fatalf("snapshot order = %v, want sorted names", got)
	}
	first.Members[0].Name = "mutated"
	second := registry.Snapshot()
	if second.Members[0].Name != "node-a" {
		t.Fatalf("snapshot mutation leaked: %#v", second)
	}
}
