//go:build mu46

package hatSql_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestDifferentialCheckpointRoundTripPreservesTypesAndOrder(t *testing.T) {
	when := time.Date(2026, time.January, 2, 3, 4, 5, 6000000, time.UTC)
	checkpoint := hatSql.DifferentialCheckpoint{
		Frontier: 42,
		Rows: []hatSql.DifferentialRow{
			{
				Key:  "z",
				Time: 9,
				Diff: -2,
				Row: hatSql.Row{
					"bytes":    []byte{1, 2, 3},
					"duration": time.Duration(17),
					"float":    float64(3.5),
					"int":      int64(-9),
					"json_num": json.Number("12345678901234567890"),
					"nested":   hatSql.Row{"ok": true, "value": "inner"},
					"text":     "hello",
					"time":     when,
					"uint":     uint64(19),
				},
			},
			{Key: "a", Time: 2, Diff: 1, Row: hatSql.Row{"value": "first"}},
		},
	}

	encoded, err := hatSql.EncodeDifferentialCheckpoint(checkpoint)
	if err != nil {
		t.Fatalf("EncodeDifferentialCheckpoint() error = %v", err)
	}
	reordered := checkpoint
	reordered.Rows = []hatSql.DifferentialRow{checkpoint.Rows[1], checkpoint.Rows[0]}
	reorderedEncoded, err := hatSql.EncodeDifferentialCheckpoint(reordered)
	if err != nil {
		t.Fatalf("EncodeDifferentialCheckpoint(reordered) error = %v", err)
	}
	if !reflect.DeepEqual(encoded, reorderedEncoded) {
		t.Fatal("checkpoint encoding depends on input row order")
	}

	decoded, err := hatSql.DecodeDifferentialCheckpoint(encoded)
	if err != nil {
		t.Fatalf("DecodeDifferentialCheckpoint() error = %v", err)
	}
	if decoded.Frontier != checkpoint.Frontier || len(decoded.Rows) != 2 {
		t.Fatalf("decoded checkpoint header = %#v, want frontier 42 and two rows", decoded)
	}
	if decoded.Rows[0].Key != "a" || decoded.Rows[1].Key != "z" {
		t.Fatalf("decoded row order = %#v, want a then z", decoded.Rows)
	}
	if !reflect.DeepEqual(decoded.Rows[0].Row, checkpoint.Rows[1].Row) {
		t.Fatalf("decoded first row = %#v, want %#v", decoded.Rows[0].Row, checkpoint.Rows[1].Row)
	}
	row := decoded.Rows[1].Row
	if got, ok := row["bytes"].([]byte); !ok || !reflect.DeepEqual(got, []byte{1, 2, 3}) {
		t.Fatalf("decoded bytes = %#v, want []byte{1, 2, 3}", row["bytes"])
	}
	if got, ok := row["duration"].(time.Duration); !ok || got != 17 {
		t.Fatalf("decoded duration = %#v, want time.Duration(17)", row["duration"])
	}
	if got, ok := row["json_num"].(json.Number); !ok || got != json.Number("12345678901234567890") {
		t.Fatalf("decoded json.Number = %#v, want original number", row["json_num"])
	}
	if got, ok := row["time"].(time.Time); !ok || !got.Equal(when) {
		t.Fatalf("decoded time = %#v, want %v", row["time"], when)
	}
	if got, ok := row["uint"].(uint64); !ok || got != 19 {
		t.Fatalf("decoded uint = %#v, want uint64(19)", row["uint"])
	}
}

func TestDifferentialCheckpointRejectsCorruptionAndBounds(t *testing.T) {
	checkpoint := hatSql.DifferentialCheckpoint{
		Frontier: 3,
		Rows:     []hatSql.DifferentialRow{{Key: "one", Time: 1, Diff: 1, Row: hatSql.Row{"value": "one"}}},
	}
	encoded, err := hatSql.EncodeDifferentialCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)/2] ^= 0x40
	if _, err := hatSql.DecodeDifferentialCheckpoint(corrupt); !errors.Is(err, hatSql.ErrDifferentialCheckpointCorrupt) {
		t.Fatalf("corrupt decode error = %v, want ErrDifferentialCheckpointCorrupt", err)
	}
	if _, err := hatSql.DecodeDifferentialCheckpoint(append(encoded, 0)); !errors.Is(err, hatSql.ErrDifferentialCheckpointCorrupt) {
		t.Fatalf("trailing decode error = %v, want ErrDifferentialCheckpointCorrupt", err)
	}
	if _, err := hatSql.EncodeDifferentialCheckpointWithOptions(
		hatSql.DifferentialCheckpoint{Frontier: 1, Rows: []hatSql.DifferentialRow{
			{Key: "one", Diff: 1},
			{Key: "two", Diff: 1},
		}},
		hatSql.DifferentialCheckpointCodecOptions{MaxRows: 1},
	); !errors.Is(err, hatSql.ErrDifferentialCheckpointTooLarge) {
		t.Fatalf("row bound error = %v, want ErrDifferentialCheckpointTooLarge", err)
	}
	if _, err := hatSql.EncodeDifferentialCheckpoint(hatSql.DifferentialCheckpoint{
		Rows: []hatSql.DifferentialRow{{Key: "unsupported", Diff: 1, Row: hatSql.Row{"value": struct{}{}}}},
	}); !errors.Is(err, hatSql.ErrDifferentialCheckpointUnsupported) {
		t.Fatalf("unsupported value error = %v, want ErrDifferentialCheckpointUnsupported", err)
	}
}

func TestDifferentialDataflowImportCheckpointIsAtomic(t *testing.T) {
	var received []hatSql.DifferentialRow
	flow, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{
		Sink: func(rows []hatSql.DifferentialRow) error {
			received = append(received, rows...)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := hatSql.DifferentialCheckpoint{
		Frontier: 10,
		Rows:     []hatSql.DifferentialRow{{Key: "one", Time: 5, Diff: 1, Row: hatSql.Row{"value": "one"}}},
	}
	encoded, err := hatSql.EncodeDifferentialCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := flow.ImportCheckpoint(encoded); err != nil {
		t.Fatalf("ImportCheckpoint() error = %v", err)
	}
	if got := flow.Frontier(); got != 10 {
		t.Fatalf("frontier after import = %d, want 10", got)
	}
	if len(received) != 1 || received[0].Key != "one" {
		t.Fatalf("received after import = %#v, want one row", received)
	}

	beforeStats := flow.Stats()
	if err := flow.ImportCheckpoint(append(encoded, 0)); !errors.Is(err, hatSql.ErrDifferentialCheckpointCorrupt) {
		t.Fatalf("corrupt import error = %v, want ErrDifferentialCheckpointCorrupt", err)
	}
	if got := flow.Frontier(); got != 10 {
		t.Fatalf("frontier after corrupt import = %d, want 10", got)
	}
	if got := flow.Stats(); !reflect.DeepEqual(got, beforeStats) {
		t.Fatalf("stats after corrupt import = %#v, want %#v", got, beforeStats)
	}
	regressive, err := hatSql.EncodeDifferentialCheckpoint(hatSql.DifferentialCheckpoint{
		Frontier: 9,
		Rows:     checkpoint.Rows,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := flow.ImportCheckpoint(regressive); !errors.Is(err, hatSql.ErrDifferentialDataflowFrontierRegression) {
		t.Fatalf("regressive import error = %v, want frontier regression", err)
	}
}

func TestDifferentialDataflowImportCheckpointSinkFailureDoesNotCommit(t *testing.T) {
	wantErr := errors.New("sink unavailable")
	flow, err := hatSql.NewDifferentialDataflow(hatSql.DifferentialDataflowOptions{
		Sink: func([]hatSql.DifferentialRow) error { return wantErr },
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := hatSql.EncodeDifferentialCheckpoint(hatSql.DifferentialCheckpoint{
		Frontier: 8,
		Rows:     []hatSql.DifferentialRow{{Key: "one", Time: 4, Diff: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := flow.ImportCheckpoint(encoded); !errors.Is(err, wantErr) {
		t.Fatalf("ImportCheckpoint() error = %v, want sink error", err)
	}
	if got := flow.Frontier(); got != 0 {
		t.Fatalf("frontier after failed import = %d, want 0", got)
	}
	if stats := flow.Stats(); stats.AppliedBatches != 0 || stats.Frontier != 0 || stats.SinkErrors != 1 {
		t.Fatalf("stats after failed import = %#v, want uncommitted batch", stats)
	}
}
