package hatStorage

import (
	"errors"
	"sync"
	"testing"
)

func TestCH020RemotePartRegistryRegistersMetadataOnly(t *testing.T) {
	registry, err := NewRemotePartRegistry(RemotePartRegistryOptions{MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	reference := ch020TestReference(t, "part-a")
	replaced, err := registry.Register(RemotePartRegistration{
		Key:        "part-a",
		Reference:  reference,
		Generation: 1,
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if replaced {
		t.Fatal("first Register() reported replacement")
	}
	got, ok := registry.Lookup("part-a")
	if !ok {
		t.Fatal("Lookup() did not find registered part")
	}
	if got.Key != "part-a" || got.Generation != 1 || got.Reference != reference {
		t.Fatalf("Lookup() = %#v, want key, generation, and reference", got)
	}
	if registry.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", registry.Len())
	}
	snapshot := registry.Snapshot()
	if len(snapshot) != 1 || snapshot[0] != got {
		t.Fatalf("Snapshot() = %#v, want %#v", snapshot, []RemotePartRegistration{got})
	}
}

func TestCH020RemotePartRegistryFencesUpdatesAndRemoval(t *testing.T) {
	registry, err := NewRemotePartRegistry(RemotePartRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	first := ch020TestReference(t, "first")
	second := ch020TestReference(t, "second")
	registration := RemotePartRegistration{Key: "part", Reference: first, Generation: 4}
	if _, err := registry.Register(registration); err != nil {
		t.Fatal(err)
	}
	if replaced, err := registry.Register(registration); err != nil || replaced {
		t.Fatalf("idempotent Register() = replaced %v, error %v", replaced, err)
	}
	if _, err := registry.Register(RemotePartRegistration{Key: "part", Reference: second, Generation: 4}); !errors.Is(err, ErrRemotePartRegistryConflict) {
		t.Fatalf("same-generation conflict error = %v, want ErrRemotePartRegistryConflict", err)
	}
	if _, err := registry.Register(RemotePartRegistration{Key: "part", Reference: second, Generation: 3}); !errors.Is(err, ErrRemotePartRegistryStale) {
		t.Fatalf("stale Register() error = %v, want ErrRemotePartRegistryStale", err)
	}
	if replaced, err := registry.Register(RemotePartRegistration{Key: "part", Reference: second, Generation: 5}); err != nil || !replaced {
		t.Fatalf("new-generation Register() = replaced %v, error %v", replaced, err)
	}
	if removed, err := registry.Unregister("part", 4); !errors.Is(err, ErrRemotePartRegistryStale) || removed {
		t.Fatalf("stale Unregister() = removed %v, error %v", removed, err)
	}
	if removed, err := registry.Unregister("part", 5); err != nil || !removed {
		t.Fatalf("matching Unregister() = removed %v, error %v", removed, err)
	}
	if _, ok := registry.Lookup("part"); ok {
		t.Fatal("Lookup() found unregistered part")
	}
}

func TestCH020RemotePartRegistryValidatesBoundsAndCapacity(t *testing.T) {
	if _, err := NewRemotePartRegistry(RemotePartRegistryOptions{MaxEntries: -1}); !errors.Is(err, ErrRemotePartRegistryInvalid) {
		t.Fatalf("negative MaxEntries error = %v, want ErrRemotePartRegistryInvalid", err)
	}
	registry, err := NewRemotePartRegistry(RemotePartRegistryOptions{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(RemotePartRegistration{Key: "", Reference: ch020TestReference(t, "empty"), Generation: 1}); !errors.Is(err, ErrRemotePartRegistryInvalid) {
		t.Fatalf("empty key error = %v, want ErrRemotePartRegistryInvalid", err)
	}
	if _, err := registry.Register(RemotePartRegistration{Key: "part", Reference: ch020TestReference(t, "zero"), Generation: 0}); !errors.Is(err, ErrRemotePartRegistryInvalid) {
		t.Fatalf("zero generation error = %v, want ErrRemotePartRegistryInvalid", err)
	}
	if _, err := registry.Register(RemotePartRegistration{Key: "zero-reference", Generation: 1}); !errors.Is(err, ErrRemotePartRegistryInvalid) {
		t.Fatalf("zero reference error = %v, want ErrRemotePartRegistryInvalid", err)
	}
	if _, err := registry.Register(RemotePartRegistration{Key: "part-a", Reference: ch020TestReference(t, "a"), Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(RemotePartRegistration{Key: "part-b", Reference: ch020TestReference(t, "b"), Generation: 1}); !errors.Is(err, ErrRemotePartRegistryFull) {
		t.Fatalf("capacity error = %v, want ErrRemotePartRegistryFull", err)
	}
}

func TestCH020RemotePartRegistryConcurrentAccess(t *testing.T) {
	registry, err := NewRemotePartRegistry(RemotePartRegistryOptions{MaxEntries: 8})
	if err != nil {
		t.Fatal(err)
	}
	references := make([]RemotePartReference, 4)
	for index := range references {
		references[index] = ch020TestReference(t, "concurrent-"+stringIntCH020(index))
	}

	var writers sync.WaitGroup
	writers.Add(len(references))
	for index, reference := range references {
		go func(index int, reference RemotePartReference) {
			defer writers.Done()
			key := "concurrent-" + stringIntCH020(index)
			for generation := uint64(1); generation <= 64; generation++ {
				if _, err := registry.Register(RemotePartRegistration{
					Key:        key,
					Reference:  reference,
					Generation: generation,
				}); err != nil {
					t.Errorf("Register(%q, generation %d) error = %v", key, generation, err)
					return
				}
			}
		}(index, reference)
	}

	var readers sync.WaitGroup
	readers.Add(4)
	for reader := 0; reader < 4; reader++ {
		go func() {
			defer readers.Done()
			for iteration := 0; iteration < 64; iteration++ {
				registry.Lookup("concurrent-0")
				registry.Len()
				registry.Snapshot()
			}
		}()
	}
	writers.Wait()
	readers.Wait()

	if registry.Len() != len(references) {
		t.Fatalf("Len() = %d, want %d", registry.Len(), len(references))
	}
}

func ch020TestReference(t *testing.T, name string) RemotePartReference {
	t.Helper()
	reference, err := NewRemotePartReference(
		"https://objects.example.test/parts/"+name+".bin",
		"parts/"+name+".bin",
		"sha256:"+name,
		4096,
	)
	if err != nil {
		t.Fatal(err)
	}
	return reference
}
