package hatDataStructure_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestT215SpacePolicySelectsEngineAndPreservesCRUDSemantics(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		options hatDataStructure.SpaceOptions
		engine  hatDataStructure.SpaceEngine
	}{
		{
			name:    "default memtx",
			options: hatDataStructure.SpaceOptions{Memtx: hatDataStructure.MemtxSpaceOptions{MaxRecords: 4}},
			engine:  hatDataStructure.SpaceEngineMemtx,
		},
		{
			name: "vinyl",
			options: hatDataStructure.SpaceOptions{
				Engine: hatDataStructure.SpaceEngineVinyl,
				Vinyl: hatDataStructure.LSMTableOptions{
					MemtableMaxRecords: 2,
					RunOptions:         hatDataStructure.SealedUpsertRunOptions{MaxRecords: 16},
				},
			},
			engine: hatDataStructure.SpaceEngineVinyl,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			space, err := hatDataStructure.NewSpace(testCase.options)
			if err != nil {
				t.Fatalf("NewSpace() error = %v", err)
			}
			if got := space.Engine(); got != testCase.engine {
				t.Fatalf("Engine() = %q, want %q", got, testCase.engine)
			}

			value := []byte("active")
			if err := space.Put("customer:42", value); err != nil {
				t.Fatalf("Put() error = %v", err)
			}
			value[0] = 'X'
			got, ok := space.Get("customer:42")
			if !ok || !reflect.DeepEqual(got, []byte("active")) {
				t.Fatalf("Get() = %q, %t, want active, true", got, ok)
			}
			if err := space.Put("customer:42", []byte("updated")); err != nil {
				t.Fatalf("replacement Put() error = %v", err)
			}
			if err := space.Delete("customer:42"); err != nil {
				t.Fatalf("Delete() error = %v", err)
			}
			if got, ok := space.Get("customer:42"); ok || got != nil {
				t.Fatalf("Get() after Delete() = %q, %t, want nil, false", got, ok)
			}
			if err := space.Flush(); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
			if err := space.Compact(); err != nil {
				t.Fatalf("Compact() error = %v", err)
			}
		})
	}
}

func TestT215SpacePolicySnapshotRoundTripAndValidation(t *testing.T) {
	for _, engine := range []hatDataStructure.SpaceEngine{
		hatDataStructure.SpaceEngineMemtx,
		hatDataStructure.SpaceEngineVinyl,
	} {
		t.Run(string(engine), func(t *testing.T) {
			options := hatDataStructure.SpaceOptions{Engine: engine}
			if engine == hatDataStructure.SpaceEngineVinyl {
				options.Vinyl.MemtableMaxRecords = 2
			}
			space, err := hatDataStructure.NewSpace(options)
			if err != nil {
				t.Fatalf("NewSpace() error = %v", err)
			}
			if err := space.Put("one", []byte("1")); err != nil {
				t.Fatal(err)
			}
			if err := space.Put("two", []byte("2")); err != nil {
				t.Fatal(err)
			}
			wire, err := space.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}
			restored, err := hatDataStructure.UnmarshalSpace(wire, options)
			if err != nil {
				t.Fatalf("UnmarshalSpace() error = %v", err)
			}
			for key, want := range map[string]string{"one": "1", "two": "2"} {
				got, ok := restored.Get(key)
				if !ok || string(got) != want {
					t.Fatalf("restored Get(%q) = %q, %t, want %q, true", key, got, ok, want)
				}
			}
			wire[0] ^= 0xff
			if _, err := hatDataStructure.UnmarshalSpace(wire, options); !errors.Is(err, hatDataStructure.ErrSpaceSnapshotCorrupt) {
				t.Fatalf("corrupt snapshot error = %v, want %v", err, hatDataStructure.ErrSpaceSnapshotCorrupt)
			}
		})
	}
}

func TestT215SpacePolicyRejectsInvalidOptionsWithoutCreatingState(t *testing.T) {
	for _, options := range []hatDataStructure.SpaceOptions{
		{Engine: hatDataStructure.SpaceEngine("unknown")},
		{Memtx: hatDataStructure.MemtxSpaceOptions{MaxRecords: -1}},
		{Engine: hatDataStructure.SpaceEngineVinyl, Vinyl: hatDataStructure.LSMTableOptions{MemtableMaxRecords: -1}},
	} {
		if _, err := hatDataStructure.NewSpace(options); err == nil {
			t.Fatalf("NewSpace(%#v) error = nil", options)
		}
	}
}

func TestT215SpacePolicyEnforcesMemtxLimitsAndEngineMatch(t *testing.T) {
	space, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
		Memtx: hatDataStructure.MemtxSpaceOptions{MaxRecords: 1, MaxValueBytes: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Put("large", []byte("123")); !errors.Is(err, hatDataStructure.ErrSpaceValueTooLarge) {
		t.Fatalf("large value error = %v, want %v", err, hatDataStructure.ErrSpaceValueTooLarge)
	}
	if err := space.Put("one", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := space.Put("two", []byte("2")); !errors.Is(err, hatDataStructure.ErrSpaceFull) {
		t.Fatalf("full space error = %v, want %v", err, hatDataStructure.ErrSpaceFull)
	}
	wire, err := space.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hatDataStructure.UnmarshalSpace(wire, hatDataStructure.SpaceOptions{Engine: hatDataStructure.SpaceEngineVinyl}); !errors.Is(err, hatDataStructure.ErrSpaceEngineMismatch) {
		t.Fatalf("engine mismatch error = %v, want %v", err, hatDataStructure.ErrSpaceEngineMismatch)
	}
}
