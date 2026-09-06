package hatReplication_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestDigestReplayRecordsIsCanonicalAndVerifiable(t *testing.T) {
	records := []hatReplication.ReplayRecord{
		{Sequence: 1, Payload: []byte("set:a")},
		{Sequence: 2, Payload: []byte("set:b")},
	}
	independent := []hatReplication.ReplayRecord{
		{Sequence: 1, Payload: []byte("set:a")},
		{Sequence: 2, Payload: []byte("set:b")},
	}
	first, err := hatReplication.DigestReplayRecords(records)
	if err != nil {
		t.Fatalf("DigestReplayRecords() error = %v", err)
	}
	second, err := hatReplication.DigestReplayRecords(independent)
	if err != nil {
		t.Fatalf("DigestReplayRecords(independent) error = %v", err)
	}
	if first != second || first.FirstSequence != 1 || first.LastSequence != 2 || first.Records != 2 {
		t.Fatalf("digests = %#v/%#v, want equal sequences 1..2", first, second)
	}
	if err := hatReplication.VerifyReplayRecords(records, independent); err != nil {
		t.Fatalf("VerifyReplayRecords() error = %v", err)
	}
	records[0].Payload[0] = 'S'
	if first != second {
		t.Fatal("digest changed after caller payload mutation")
	}
}

func TestDigestReplayRecordsRejectsInvalidOrderAndReportsMismatch(t *testing.T) {
	for name, records := range map[string][]hatReplication.ReplayRecord{
		"zero sequence":      {{Payload: []byte("x")}},
		"regressed sequence": {{Sequence: 2}, {Sequence: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := hatReplication.DigestReplayRecords(records); !errors.Is(err, hatReplication.ErrReplaySequenceInvalid) {
				t.Fatalf("DigestReplayRecords() error = %v, want ErrReplaySequenceInvalid", err)
			}
		})
	}
	left := []hatReplication.ReplayRecord{{Sequence: 1, Payload: []byte("left")}}
	right := []hatReplication.ReplayRecord{{Sequence: 1, Payload: []byte("right")}}
	if err := hatReplication.VerifyReplayRecords(left, right); !errors.Is(err, hatReplication.ErrReplayMismatch) {
		t.Fatalf("VerifyReplayRecords() error = %v, want ErrReplayMismatch", err)
	}
}

func TestDigestReplayRecordsSupportsEmptyInput(t *testing.T) {
	digest, err := hatReplication.DigestReplayRecords(nil)
	if err != nil {
		t.Fatalf("DigestReplayRecords(nil) error = %v", err)
	}
	if digest.Records != 0 || digest.FirstSequence != 0 || digest.LastSequence != 0 {
		t.Fatalf("empty digest = %#v, want zero metadata", digest)
	}
}

func BenchmarkDigestReplayRecords(b *testing.B) {
	records := make([]hatReplication.ReplayRecord, 1024)
	for index := range records {
		records[index] = hatReplication.ReplayRecord{Sequence: uint64(index + 1), Payload: []byte("set:payload")}
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if _, err := hatReplication.DigestReplayRecords(records); err != nil {
			b.Fatal(err)
		}
	}
}
