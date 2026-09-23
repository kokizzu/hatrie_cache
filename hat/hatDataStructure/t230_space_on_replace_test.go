package hatDataStructure_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestT230OnReplaceReceivesSuccessfulEventsForBothEngines(t *testing.T) {
	for _, engine := range []hatDataStructure.SpaceEngine{
		hatDataStructure.SpaceEngineMemtx,
		hatDataStructure.SpaceEngineVinyl,
	} {
		t.Run(string(engine), func(t *testing.T) {
			var events []hatDataStructure.SpaceReplace
			options := t230SpaceOptions(engine)
			var space *hatDataStructure.Space
			options.OnReplace = func(event hatDataStructure.SpaceReplace) {
				events = append(events, hatDataStructure.SpaceReplace{
					Key:      event.Key,
					OldValue: append([]byte(nil), event.OldValue...),
					NewValue: append([]byte(nil), event.NewValue...),
					Exists:   event.Exists,
					Delete:   event.Delete,
				})
				if len(event.OldValue) > 0 {
					event.OldValue[0] = 'x'
				}
				if len(event.NewValue) > 0 {
					event.NewValue[0] = 'x'
				}
			}
			var err error
			space, err = hatDataStructure.NewSpace(options)
			if err != nil {
				t.Fatal(err)
			}
			if err := space.Put("key", []byte("one")); err != nil {
				t.Fatal(err)
			}
			if got, ok := space.Get("key"); !ok || string(got) != "one" {
				t.Fatalf("stored value after callback mutation = %q, %t, want one, true", got, ok)
			}
			if err := space.Put("key", []byte("two")); err != nil {
				t.Fatal(err)
			}
			if err := space.Delete("key"); err != nil {
				t.Fatal(err)
			}
			want := []hatDataStructure.SpaceReplace{
				{Key: "key", NewValue: []byte("one")},
				{Key: "key", OldValue: []byte("one"), NewValue: []byte("two"), Exists: true},
				{Key: "key", OldValue: []byte("two"), Exists: true, Delete: true},
			}
			if !reflect.DeepEqual(events, want) {
				t.Fatalf("events = %#v, want %#v", events, want)
			}
		})
	}
}

func TestT230OnReplaceRunsOnlyAfterSuccessfulMutation(t *testing.T) {
	conflict := errors.New("conflict")
	called := 0
	options := t230SpaceOptions(hatDataStructure.SpaceEngineMemtx)
	options.Memtx = hatDataStructure.MemtxSpaceOptions{MaxRecords: 1, MaxValueBytes: 3}
	options.BeforeReplace = func(event hatDataStructure.SpaceReplace) error {
		if event.Exists {
			return conflict
		}
		return nil
	}
	options.OnReplace = func(hatDataStructure.SpaceReplace) { called++ }
	space, err := hatDataStructure.NewSpace(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Put("large", []byte("1234")); !errors.Is(err, hatDataStructure.ErrSpaceValueTooLarge) {
		t.Fatalf("oversized value error = %v, want %v", err, hatDataStructure.ErrSpaceValueTooLarge)
	}
	if err := space.Put("key", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := space.Put("key", []byte("two")); !errors.Is(err, conflict) {
		t.Fatalf("conflict error = %v, want %v", err, conflict)
	}
	if err := space.Delete("missing"); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("OnReplace call count = %d, want 1", called)
	}
	got, ok := space.Get("key")
	if !ok || string(got) != "one" {
		t.Fatalf("stored value = %q, %t, want one, true", got, ok)
	}
}

func TestT230OnReplaceSkipsInvalidVinylValue(t *testing.T) {
	called := 0
	space, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
		Engine: hatDataStructure.SpaceEngineVinyl,
		Vinyl: hatDataStructure.LSMTableOptions{
			RunOptions: hatDataStructure.SealedUpsertRunOptions{MaxValueBytes: 2},
		},
		OnReplace: func(hatDataStructure.SpaceReplace) { called++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Put("key", []byte("123")); !errors.Is(err, hatDataStructure.ErrLSMTableValueTooLarge) {
		t.Fatalf("oversized Vinyl value error = %v, want %v", err, hatDataStructure.ErrLSMTableValueTooLarge)
	}
	if called != 0 {
		t.Fatalf("OnReplace call count after invalid Vinyl value = %d, want 0", called)
	}
}

func BenchmarkT230SpaceOnReplace(b *testing.B) {
	space, err := t230BenchmarkSpace(b)
	if err != nil {
		b.Fatal(err)
	}
	value := []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := space.Put("key", value); err != nil {
			b.Fatal(err)
		}
	}
}

func t230SpaceOptions(engine hatDataStructure.SpaceEngine) hatDataStructure.SpaceOptions {
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

func t230BenchmarkSpace(b testing.TB) (*hatDataStructure.Space, error) {
	options := t230SpaceOptions(hatDataStructure.SpaceEngineMemtx)
	options.OnReplace = func(hatDataStructure.SpaceReplace) {}
	return hatDataStructure.NewSpace(options)
}
