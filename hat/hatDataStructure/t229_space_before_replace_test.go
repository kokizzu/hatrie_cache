package hatDataStructure_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestT229BeforeReplaceReceivesCopiedStateForBothEngines(t *testing.T) {
	for _, engine := range []hatDataStructure.SpaceEngine{
		hatDataStructure.SpaceEngineMemtx,
		hatDataStructure.SpaceEngineVinyl,
	} {
		t.Run(string(engine), func(t *testing.T) {
			var events []hatDataStructure.SpaceReplace
			options := t229SpaceOptions(engine)
			options.BeforeReplace = func(event hatDataStructure.SpaceReplace) error {
				events = append(events, hatDataStructure.SpaceReplace{
					Key:      event.Key,
					OldValue: append([]byte(nil), event.OldValue...),
					NewValue: append([]byte(nil), event.NewValue...),
					Exists:   event.Exists,
					Delete:   event.Delete,
				})
				if len(event.OldValue) > 0 {
					event.OldValue[0] = 'X'
				}
				if len(event.NewValue) > 0 {
					event.NewValue[0] = 'X'
				}
				return nil
			}
			space, err := hatDataStructure.NewSpace(options)
			if err != nil {
				t.Fatalf("NewSpace() error = %v", err)
			}
			initial := []byte("initial")
			if err := space.Put("key", initial); err != nil {
				t.Fatalf("initial Put() error = %v", err)
			}
			if !reflect.DeepEqual(initial, []byte("initial")) {
				t.Fatalf("callback mutated input value: %q", initial)
			}
			if err := space.Put("key", []byte("updated")); err != nil {
				t.Fatalf("replacement Put() error = %v", err)
			}
			if err := space.Delete("key"); err != nil {
				t.Fatalf("Delete() error = %v", err)
			}
			if len(events) != 3 {
				t.Fatalf("callback event count = %d, want 3", len(events))
			}
			if events[0].Key != "key" || events[0].Exists || events[0].Delete || string(events[0].NewValue) != "initial" || events[0].OldValue != nil {
				t.Fatalf("insert event = %#v", events[0])
			}
			if !events[1].Exists || events[1].Delete || string(events[1].OldValue) != "initial" || string(events[1].NewValue) != "updated" {
				t.Fatalf("replace event = %#v", events[1])
			}
			if !events[2].Exists || !events[2].Delete || string(events[2].OldValue) != "updated" || events[2].NewValue != nil {
				t.Fatalf("delete event = %#v", events[2])
			}
		})
	}
}

func TestT229BeforeReplaceRejectsConflictsWithoutMutation(t *testing.T) {
	conflict := errors.New("write conflict")
	for _, engine := range []hatDataStructure.SpaceEngine{
		hatDataStructure.SpaceEngineMemtx,
		hatDataStructure.SpaceEngineVinyl,
	} {
		t.Run(string(engine), func(t *testing.T) {
			options := t229SpaceOptions(engine)
			options.BeforeReplace = func(event hatDataStructure.SpaceReplace) error {
				if event.Exists {
					return conflict
				}
				return nil
			}
			space, err := hatDataStructure.NewSpace(options)
			if err != nil {
				t.Fatalf("NewSpace() error = %v", err)
			}
			if err := space.Put("key", []byte("initial")); err != nil {
				t.Fatalf("initial Put() error = %v", err)
			}
			if err := space.Put("key", []byte("updated")); !errors.Is(err, conflict) {
				t.Fatalf("replacement error = %v, want %v", err, conflict)
			}
			if err := space.Delete("key"); !errors.Is(err, conflict) {
				t.Fatalf("delete error = %v, want %v", err, conflict)
			}
			value, ok := space.Get("key")
			if !ok || string(value) != "initial" {
				t.Fatalf("value after rejected writes = %q, %t, want initial, true", value, ok)
			}
		})
	}
}

func TestT229BeforeReplaceRunsAfterBasicInputValidation(t *testing.T) {
	called := 0
	space, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
		Memtx: hatDataStructure.MemtxSpaceOptions{MaxRecords: 1, MaxValueBytes: 2},
		BeforeReplace: func(hatDataStructure.SpaceReplace) error {
			called++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewSpace() error = %v", err)
	}
	if err := space.Put("", []byte("ok")); !errors.Is(err, hatDataStructure.ErrSpaceKeyRequired) {
		t.Fatalf("empty key error = %v, want %v", err, hatDataStructure.ErrSpaceKeyRequired)
	}
	if err := space.Put("large", []byte("123")); !errors.Is(err, hatDataStructure.ErrSpaceValueTooLarge) {
		t.Fatalf("large value error = %v, want %v", err, hatDataStructure.ErrSpaceValueTooLarge)
	}
	if called != 0 {
		t.Fatalf("callback called %d times for invalid inputs, want 0", called)
	}
}

func TestT229BeforeReplaceRunsAfterVinylValueValidation(t *testing.T) {
	called := 0
	space, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
		Engine: hatDataStructure.SpaceEngineVinyl,
		Vinyl: hatDataStructure.LSMTableOptions{
			RunOptions: hatDataStructure.SealedUpsertRunOptions{MaxValueBytes: 2},
		},
		BeforeReplace: func(hatDataStructure.SpaceReplace) error {
			called++
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Put("key", []byte("123")); !errors.Is(err, hatDataStructure.ErrLSMTableValueTooLarge) {
		t.Fatalf("oversized Vinyl value error = %v, want %v", err, hatDataStructure.ErrLSMTableValueTooLarge)
	}
	if called != 0 {
		t.Fatalf("callback count after invalid Vinyl value = %d, want 0", called)
	}
}

func BenchmarkT229SpaceBeforeReplace(b *testing.B) {
	space, err := t229BenchmarkSpace(b)
	if err != nil {
		b.Fatal(err)
	}
	value := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := space.Put("key", value); err != nil {
			b.Fatal(err)
		}
	}
}

func t229SpaceOptions(engine hatDataStructure.SpaceEngine) hatDataStructure.SpaceOptions {
	options := hatDataStructure.SpaceOptions{Engine: engine}
	if engine == hatDataStructure.SpaceEngineMemtx {
		options.Memtx = hatDataStructure.MemtxSpaceOptions{MaxRecords: 8, MaxValueBytes: 64}
	} else {
		options.Vinyl = hatDataStructure.LSMTableOptions{
			MemtableMaxRecords: 1 << 20,
			RunOptions:         hatDataStructure.SealedUpsertRunOptions{MaxRecords: 1 << 20},
		}
	}
	return options
}

func t229BenchmarkSpace(b testing.TB) (*hatDataStructure.Space, error) {
	options := t229SpaceOptions(hatDataStructure.SpaceEngineMemtx)
	options.BeforeReplace = func(hatDataStructure.SpaceReplace) error { return nil }
	return hatDataStructure.NewSpace(options)
}
