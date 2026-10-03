package hatReplication

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"testing"
)

func TestT038ConflictEventLogRedactsBoundsAndRestores(t *testing.T) {
	options := ConflictEventLogOptions{
		Capacity: 2,
		MaxBytes: 4096,
		HashKey:  []byte("t038-conflict-log-secret"),
	}
	log, err := NewConflictEventLog(options)
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 1}
	first, err := log.Record("payments", []byte("secret-payment-key"), left, right, ConflictPolicyLastWriteWins, right, ConflictEventResolved)
	if err != nil {
		t.Fatalf("Record(first) error = %v", err)
	}
	if first.Sequence != 1 || first.Space != "payments" || first.KeyDigest == ([16]byte{}) {
		t.Fatalf("first event = %#v, want sequence, space, and redacted digest", first)
	}
	encoded, err := log.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	if bytes.Contains(encoded, []byte("secret-payment-key")) {
		t.Fatal("serialized conflict event contains the raw key")
	}

	if _, err := log.Record("payments", []byte("second"), right, left, ConflictPolicyReject, ConflictVersion{}, ConflictEventRejected); err != nil {
		t.Fatalf("Record(second) error = %v", err)
	}
	third, err := log.Record("payments", []byte("third"), left, right, ConflictPolicySourcePriority, left, ConflictEventResolved)
	if err != nil {
		t.Fatalf("Record(third) error = %v", err)
	}
	if third.Sequence != 3 {
		t.Fatalf("third sequence = %d, want 3", third.Sequence)
	}

	if _, err := log.ReadAfter(0, 10); !errors.Is(err, ErrConflictEventCursorExpired) {
		t.Fatalf("ReadAfter(expired) error = %v, want ErrConflictEventCursorExpired", err)
	}
	page, err := log.ReadAfter(1, 10)
	if err != nil {
		t.Fatalf("ReadAfter(1) error = %v", err)
	}
	if page.OldestSequence != 2 || page.NewestSequence != 3 || page.NextSequence != 3 || page.More || len(page.Events) != 2 {
		t.Fatalf("page = %#v, want retained sequences 2..3", page)
	}
	if page.Events[0].Sequence != 2 || page.Events[1].Sequence != 3 {
		t.Fatalf("page events = %#v, want ordered retained events", page.Events)
	}

	restored, err := NewConflictEventLogFromBinary(encoded, options)
	if err != nil {
		t.Fatalf("NewConflictEventLogFromBinary() error = %v", err)
	}
	restoredPage, err := restored.ReadAfter(0, 10)
	if err != nil {
		t.Fatalf("restored ReadAfter() error = %v", err)
	}
	if len(restoredPage.Events) != 1 || restoredPage.Events[0].Sequence != 1 || restoredPage.Events[0].KeyDigest != first.KeyDigest {
		t.Fatalf("restored page = %#v, want first event with stable digest", restoredPage)
	}
	next, err := restored.Record("payments", []byte("after-restore"), right, left, ConflictPolicyLastWriteWins, right, ConflictEventResolved)
	if err != nil {
		t.Fatalf("Record(after restore) error = %v", err)
	}
	if next.Sequence != 2 {
		t.Fatalf("after-restore sequence = %d, want 2", next.Sequence)
	}
}

func TestT038ConflictEventLogRejectsInvalidAndCorruptState(t *testing.T) {
	if _, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 0, MaxBytes: 128, HashKey: []byte("secret")}); !errors.Is(err, ErrConflictEventLogInvalid) {
		t.Fatalf("invalid capacity error = %v, want ErrConflictEventLogInvalid", err)
	}
	if _, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 2, MaxBytes: 128, HashKey: []byte("short")}); !errors.Is(err, ErrConflictEventLogInvalid) {
		t.Fatalf("invalid max bytes error = %v, want ErrConflictEventLogInvalid", err)
	}
	if _, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 2, MaxBytes: 4096, HashKey: []byte("short")}); !errors.Is(err, ErrConflictEventLogInvalid) {
		t.Fatalf("invalid hash key error = %v, want ErrConflictEventLogInvalid", err)
	}

	options := ConflictEventLogOptions{Capacity: 2, MaxBytes: 4096, HashKey: []byte("t038-corruption-secret")}
	log, err := NewConflictEventLog(options)
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	if _, err := log.Record("orders", []byte("key"), ConflictVersion{Timestamp: 1, NodeID: "a"}, ConflictVersion{Timestamp: 2, NodeID: "b"}, ConflictPolicyLastWriteWins, ConflictVersion{Timestamp: 2, NodeID: "b"}, ConflictEventResolved); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	encoded, err := log.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	encoded[len(encoded)-1] ^= 0xff
	if _, err := NewConflictEventLogFromBinary(encoded, options); !errors.Is(err, ErrConflictEventLogCorrupt) {
		t.Fatalf("corrupt snapshot error = %v, want ErrConflictEventLogCorrupt", err)
	}
}

func TestT038ConflictEventLogDigestMatchesHMAC(t *testing.T) {
	secret := []byte("t038-digest-secret")
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 2, MaxBytes: 4096, HashKey: secret})
	if err != nil {
		t.Fatalf("NewConflictEventLog() error = %v", err)
	}
	key := bytes.Repeat([]byte("k"), 96)
	left := ConflictVersion{Timestamp: 1, NodeID: "left"}
	right := ConflictVersion{Timestamp: 2, NodeID: "right"}
	event, err := log.Record("orders", key, left, right, ConflictPolicyLastWriteWins, right, ConflictEventResolved)
	if err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	normalizedKey := sha256.Sum256(secret)
	mac := hmac.New(sha256.New, normalizedKey[:])
	_, _ = mac.Write([]byte(conflictEventDigestDomain))
	_, _ = mac.Write(key)
	var want [conflictEventLogKeyDigestBytes]byte
	copy(want[:], mac.Sum(nil)[:conflictEventLogKeyDigestBytes])
	if event.KeyDigest != want {
		t.Fatalf("KeyDigest = %x, want HMAC prefix %x", event.KeyDigest, want)
	}
}

func BenchmarkT038ConflictEventLog(b *testing.B) {
	left := ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 1}
	key := []byte("benchmark-key")
	b.Run("resolution_only", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := ResolveConflictVersion(left, right); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("resolution_plus_log", func(b *testing.B) {
		log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 4096, MaxBytes: 1 << 20, HashKey: []byte("t038-benchmark-secret")})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			winner, err := ResolveConflictVersion(left, right)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := log.Record("payments", key, left, right, ConflictPolicyLastWriteWins, winner, ConflictEventResolved); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkT038ConflictEventLogSnapshot(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 128, MaxBytes: 1 << 20, HashKey: []byte("t038-snapshot-secret")})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 1}
	for index := 0; index < 128; index++ {
		if _, err := log.Record("payments", []byte("benchmark-key"), left, right, ConflictPolicyLastWriteWins, right, ConflictEventResolved); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(len(mustConflictEventSnapshot(b, log))), "snapshot-bytes/op")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := log.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func mustConflictEventSnapshot(b *testing.B, log *ConflictEventLog) []byte {
	b.Helper()
	data, err := log.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	return data
}
