package hatSql_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestMZ021DerivedSinkTokenIsCanonicalAndRetryStable(t *testing.T) {
	first := hatSql.SQLSinkIdempotencyTokenInput{
		Sink: "warehouse",
		Progress: []hatSql.SQLSinkProgress{
			{Sink: "warehouse", Partition: "1", Frontier: 20},
			{Sink: "warehouse", Partition: "0", Frontier: 10},
		},
		Batch: []byte(`{"id":1,"value":"Ada"}`),
	}
	second := first
	second.Progress = []hatSql.SQLSinkProgress{first.Progress[1], first.Progress[0]}
	firstKey, err := hatSql.DeriveSQLSinkIdempotencyKey(first)
	if err != nil {
		t.Fatalf("first DeriveSQLSinkIdempotencyKey() error = %v", err)
	}
	secondKey, err := hatSql.DeriveSQLSinkIdempotencyKey(second)
	if err != nil {
		t.Fatalf("second DeriveSQLSinkIdempotencyKey() error = %v", err)
	}
	if firstKey != secondKey || len(firstKey) != 64 {
		t.Fatalf("canonical keys = %q/%q, want equal 64-character digests", firstKey, secondKey)
	}
	if bytes.Contains([]byte(firstKey), first.Batch) {
		t.Fatal("derived key contains the batch payload")
	}

	commit := hatSql.SQLSinkCommit{
		Sink:          "warehouse",
		TransactionID: "generated-attempt-1",
		Progress:      first.Progress,
	}
	derived, err := hatSql.DeriveSQLSinkCommitIdempotencyKey(commit, first.Batch)
	if err != nil {
		t.Fatalf("DeriveSQLSinkCommitIdempotencyKey() error = %v", err)
	}
	if derived.IdempotencyKey != firstKey {
		t.Fatalf("derived commit key = %q, want %q", derived.IdempotencyKey, firstKey)
	}
	if derived.TransactionID != commit.TransactionID || len(derived.Progress) != len(commit.Progress) {
		t.Fatalf("derived commit changed identity/progress = %#v", derived)
	}

	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(context.Background(), hatSql.SQLSinkExactlyOnceOptions{Capacity: 8})
	if err != nil {
		t.Fatalf("NewSQLSinkExactlyOnceLedger() error = %v", err)
	}
	calls := 0
	if committed, err := ledger.Commit(derived, func(string) error { calls++; return nil }); err != nil || !committed {
		t.Fatalf("first derived Commit() = %t/%v, want true/nil", committed, err)
	}
	retry := derived
	retry.TransactionID = "generated-attempt-2"
	if committed, err := ledger.Commit(retry, func(string) error { calls++; return nil }); err != nil || committed {
		t.Fatalf("retry derived Commit() = %t/%v, want false/nil", committed, err)
	}
	if calls != 1 {
		t.Fatalf("retry callback calls = %d, want 1", calls)
	}
}

func TestMZ021DerivedSinkTokenChangesWithInput(t *testing.T) {
	base := hatSql.SQLSinkIdempotencyTokenInput{
		Sink:     "warehouse",
		Progress: []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 1}},
		Batch:    []byte("row-1"),
	}
	baseKey, err := hatSql.DeriveSQLSinkIdempotencyKey(base)
	if err != nil {
		t.Fatalf("base token error = %v", err)
	}
	for name, changed := range map[string]hatSql.SQLSinkIdempotencyTokenInput{
		"sink":     {Sink: "other", Progress: []hatSql.SQLSinkProgress{{Sink: "other", Partition: "0", Frontier: 1}}, Batch: base.Batch},
		"frontier": {Sink: base.Sink, Progress: []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 2}}, Batch: base.Batch},
		"batch":    {Sink: base.Sink, Progress: base.Progress, Batch: []byte("row-2")},
	} {
		key, err := hatSql.DeriveSQLSinkIdempotencyKey(changed)
		if err != nil {
			t.Fatalf("%s token error = %v", name, err)
		}
		if key == baseKey {
			t.Fatalf("%s token = %q, want a distinct digest", name, key)
		}
	}
}

func TestMZ021DerivedSinkTokenRejectsMalformedInput(t *testing.T) {
	valid := hatSql.SQLSinkIdempotencyTokenInput{
		Sink:     "warehouse",
		Progress: []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 1}},
		Batch:    []byte("row"),
	}
	cases := map[string]hatSql.SQLSinkIdempotencyTokenInput{
		"missing sink":     {Progress: valid.Progress, Batch: valid.Batch},
		"missing progress": {Sink: valid.Sink, Batch: valid.Batch},
		"wrong progress sink": {
			Sink:     valid.Sink,
			Progress: []hatSql.SQLSinkProgress{{Sink: "other", Partition: "0", Frontier: 1}},
			Batch:    valid.Batch,
		},
		"duplicate partition": {
			Sink: valid.Sink,
			Progress: []hatSql.SQLSinkProgress{
				{Sink: valid.Sink, Partition: "0", Frontier: 1},
				{Sink: valid.Sink, Partition: "0", Frontier: 2},
			},
			Batch: valid.Batch,
		},
	}
	for name, input := range cases {
		if _, err := hatSql.DeriveSQLSinkIdempotencyKey(input); !errors.Is(err, hatSql.ErrSQLSinkIdempotencyTokenInvalid) {
			t.Errorf("%s error = %v, want invalid-token error", name, err)
		}
	}
}

func TestMZ021DerivedSinkTokenBoundsAndStreamingPath(t *testing.T) {
	input := hatSql.SQLSinkIdempotencyTokenInput{
		Sink:     "warehouse",
		Progress: []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 1}},
		Batch:    bytes.Repeat([]byte{0xa5}, 2048),
	}
	first, err := hatSql.DeriveSQLSinkIdempotencyKey(input)
	if err != nil {
		t.Fatalf("large DeriveSQLSinkIdempotencyKey() error = %v", err)
	}
	second, err := hatSql.DeriveSQLSinkIdempotencyKey(input)
	if err != nil {
		t.Fatalf("large retry DeriveSQLSinkIdempotencyKey() error = %v", err)
	}
	if first != second || len(first) != 64 {
		t.Fatalf("large canonical keys = %q/%q, want equal 64-character digests", first, second)
	}

	tooLarge := input
	tooLarge.Batch = make([]byte, hatSql.MaxSQLSinkIdempotencyBatchBytes+1)
	if _, err := hatSql.DeriveSQLSinkIdempotencyKey(tooLarge); !errors.Is(err, hatSql.ErrSQLSinkIdempotencyTokenLimit) {
		t.Fatalf("oversized batch error = %v, want limit", err)
	}

	tooManyPartitions := input
	tooManyPartitions.Progress = make([]hatSql.SQLSinkProgress, hatSql.MaxSQLSinkIdempotencyProgress+1)
	if _, err := hatSql.DeriveSQLSinkIdempotencyKey(tooManyPartitions); !errors.Is(err, hatSql.ErrSQLSinkIdempotencyTokenLimit) {
		t.Fatalf("oversized progress error = %v, want limit", err)
	}
}
