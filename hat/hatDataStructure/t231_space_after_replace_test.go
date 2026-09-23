package hatDataStructure_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestT231AfterReplaceCarriesMonotoneTransactionIdentity(t *testing.T) {
	for _, engine := range []hatDataStructure.SpaceEngine{
		hatDataStructure.SpaceEngineMemtx,
		hatDataStructure.SpaceEngineVinyl,
	} {
		t.Run(string(engine), func(t *testing.T) {
			var audits []hatDataStructure.SpaceReplaceAudit
			options := t231SpaceOptions(engine)
			options.AfterReplace = func(audit hatDataStructure.SpaceReplaceAudit) {
				audits = append(audits, hatDataStructure.SpaceReplaceAudit{
					TransactionID: audit.TransactionID,
					SpaceReplace: hatDataStructure.SpaceReplace{
						Key:      audit.Key,
						OldValue: append([]byte(nil), audit.OldValue...),
						NewValue: append([]byte(nil), audit.NewValue...),
						Exists:   audit.Exists,
						Delete:   audit.Delete,
					},
				})
				if len(audit.OldValue) > 0 {
					audit.OldValue[0] = 'x'
				}
				if len(audit.NewValue) > 0 {
					audit.NewValue[0] = 'x'
				}
			}
			space, err := hatDataStructure.NewSpace(options)
			if err != nil {
				t.Fatal(err)
			}
			if err := space.Put("key", []byte("one")); err != nil {
				t.Fatal(err)
			}
			if got, ok := space.Get("key"); !ok || string(got) != "one" {
				t.Fatalf("stored value after audit mutation = %q, %t, want one, true", got, ok)
			}
			if err := space.Put("key", []byte("two")); err != nil {
				t.Fatal(err)
			}
			if err := space.Delete("key"); err != nil {
				t.Fatal(err)
			}
			want := []hatDataStructure.SpaceReplaceAudit{
				{TransactionID: 1, SpaceReplace: hatDataStructure.SpaceReplace{Key: "key", NewValue: []byte("one")}},
				{TransactionID: 2, SpaceReplace: hatDataStructure.SpaceReplace{Key: "key", OldValue: []byte("one"), NewValue: []byte("two"), Exists: true}},
				{TransactionID: 3, SpaceReplace: hatDataStructure.SpaceReplace{Key: "key", OldValue: []byte("two"), Exists: true, Delete: true}},
			}
			if !reflect.DeepEqual(audits, want) {
				t.Fatalf("audits = %#v, want %#v", audits, want)
			}
		})
	}
}

func TestT231AfterReplaceSkipsRejectedAndMissingMutations(t *testing.T) {
	conflict := errors.New("conflict")
	var audits []hatDataStructure.SpaceReplaceAudit
	options := t231SpaceOptions(hatDataStructure.SpaceEngineMemtx)
	options.Memtx = hatDataStructure.MemtxSpaceOptions{MaxRecords: 1, MaxValueBytes: 3}
	options.BeforeReplace = func(event hatDataStructure.SpaceReplace) error {
		if event.Exists && !event.Delete {
			return conflict
		}
		return nil
	}
	options.AfterReplace = func(audit hatDataStructure.SpaceReplaceAudit) { audits = append(audits, audit) }
	space, err := hatDataStructure.NewSpace(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Put("key", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := space.Put("key", []byte("two")); !errors.Is(err, conflict) {
		t.Fatalf("conflict error = %v, want %v", err, conflict)
	}
	if err := space.Put("large", []byte("1234")); !errors.Is(err, hatDataStructure.ErrSpaceValueTooLarge) {
		t.Fatalf("oversized value error = %v, want %v", err, hatDataStructure.ErrSpaceValueTooLarge)
	}
	if err := space.Delete("missing"); err != nil {
		t.Fatal(err)
	}
	if err := space.Delete("key"); err != nil {
		t.Fatal(err)
	}
	if len(audits) != 2 || audits[0].TransactionID != 1 || audits[1].TransactionID != 2 {
		t.Fatalf("successful audit IDs = %#v, want 1, 2", audits)
	}
}

func BenchmarkT231SpaceAfterReplace(b *testing.B) {
	space, err := t231BenchmarkSpace(b)
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

func t231SpaceOptions(engine hatDataStructure.SpaceEngine) hatDataStructure.SpaceOptions {
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

func t231BenchmarkSpace(b testing.TB) (*hatDataStructure.Space, error) {
	options := t231SpaceOptions(hatDataStructure.SpaceEngineMemtx)
	options.AfterReplace = func(hatDataStructure.SpaceReplaceAudit) {}
	return hatDataStructure.NewSpace(options)
}
