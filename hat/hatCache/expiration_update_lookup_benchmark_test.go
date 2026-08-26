package hatCache

import (
	"testing"
	"time"
)

func BenchmarkExpirationUpdateLookup(b *testing.B) {
	fixedNow := time.Unix(1_700_000_000, 0)

	b.Run("ExistingEqual", func(b *testing.B) {
		ht := CreateHatTrie()
		defer ht.Destroy()
		ht.now = func() time.Time { return fixedNow }
		ht.UpsertString("expiration:equal", "value")
		ht.Expire("expiration:equal", time.Hour)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if !ht.Expire("expiration:equal", time.Hour) {
				b.Fatal("Expire(existing equal deadline) = false")
			}
		}
	})

	b.Run("ExistingLater", func(b *testing.B) {
		ht := CreateHatTrie()
		defer ht.Destroy()
		ht.now = func() time.Time { return fixedNow }
		ht.UpsertString("expiration:later", "value")
		at := fixedNow.Add(time.Hour)
		ht.ExpireAt("expiration:later", at)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			at = at.Add(time.Nanosecond)
			if !ht.ExpireAt("expiration:later", at) {
				b.Fatal("ExpireAt(existing later deadline) = false")
			}
		}
	})

	b.Run("FirstScheduleClear", func(b *testing.B) {
		ht := CreateHatTrie()
		defer ht.Destroy()
		ht.now = func() time.Time { return fixedNow }
		ht.UpsertString("expiration:first", "value")
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if !ht.Expire("expiration:first", time.Hour) {
				b.Fatal("Expire(first schedule) = false")
			}
			if !ht.Persist("expiration:first") {
				b.Fatal("Persist(scheduled expiration) = false")
			}
		}
	})
}
