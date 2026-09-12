package hatReplication

import "testing"

func BenchmarkChangefeedCheckpointOperations(b *testing.B) {
	checkpoint, err := NewChangefeedCheckpoint("orders", 42)
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := checkpoint.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	progress := ChangefeedProgress{Sequence: 43, Progressed: true}

	b.Run("advance", func(b *testing.B) {
		b.ReportAllocs()
		current := checkpoint
		for index := 0; index < b.N; index++ {
			current, err = current.Advance(ChangefeedProgress{Sequence: uint64(index + 43), Progressed: true})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("marshal", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := checkpoint.MarshalBinary(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := UnmarshalChangefeedCheckpoint(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("advance-prepared", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := checkpoint.Advance(progress); err != nil {
				b.Fatal(err)
			}
		}
	})
}
