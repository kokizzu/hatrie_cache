package hatCache

import (
	"encoding/json"
	"testing"
)

var benchmarkCommandJournalCursorBytes []byte

func BenchmarkCommandJournalCursorCodec(b *testing.B) {
	cursor, err := newCommandJournalCursorCodec("0123456789abcdef")
	if err != nil {
		b.Fatalf("newCommandJournalCursorCodec() error = %v", err)
	}
	journal := &CommandJournal{path: "/tmp/hatrie-cache-benchmark/commands.journal"}
	token, err := cursor.encode(journal, 12345)
	if err != nil {
		b.Fatalf("encode() error = %v", err)
	}

	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(token)), "token-B")
		for index := 0; index < b.N; index++ {
			encoded, err := cursor.encode(journal, uint64(index+1))
			if err != nil {
				b.Fatalf("encode() error = %v", err)
			}
			benchmarkCommandJournalCursorBytes = []byte(encoded)
		}
	})

	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			sequence, err := cursor.decode(journal, token)
			if err != nil || sequence != 12345 {
				b.Fatalf("decode() = %d/%v, want 12345/nil", sequence, err)
			}
		}
	})
}

func BenchmarkCommandJournalTailJSONCursorEnvelope(b *testing.B) {
	cursor, err := newCommandJournalCursorCodec("0123456789abcdef")
	if err != nil {
		b.Fatalf("newCommandJournalCursorCodec() error = %v", err)
	}
	journal := &CommandJournal{path: "/tmp/hatrie-cache-benchmark/commands.journal"}
	token, err := cursor.encode(journal, 12345)
	if err != nil {
		b.Fatalf("encode() error = %v", err)
	}
	base := CommandJournalTail{
		LastSequence: 12346,
		Limit:        1,
		HasMore:      true,
		Entries: []CommandJournalRecord{{
			Sequence: 12345,
			Request: CacheCommandRequest{
				Command: "SETSTR",
				Key:     "cursor:key",
				Value:   "value",
			},
		}},
	}

	for _, test := range []struct {
		name   string
		cursor bool
	}{{name: "disabled"}, {name: "enabled", cursor: true}} {
		b.Run(test.name, func(b *testing.B) {
			tail := base
			if test.cursor {
				tail.NextCursor = token
			}
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				data, err := json.Marshal(tail)
				if err != nil {
					b.Fatalf("json.Marshal() error = %v", err)
				}
				benchmarkCommandJournalCursorBytes = data
			}
			b.ReportMetric(float64(len(benchmarkCommandJournalCursorBytes)), "wire-B")
		})
	}
}
