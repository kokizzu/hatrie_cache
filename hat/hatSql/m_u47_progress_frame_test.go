//go:build mu47

package hatSql_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestQuerySubscriptionProgressFrameRoundTrip(t *testing.T) {
	batch := hatSql.QuerySubscriptionDeltaBatch{
		ID:       7,
		Revision: 42,
		Frontier: 9001,
		Progress: true,
		Complete: true,
	}
	payload, err := hatSql.EncodeQuerySubscriptionProgressFrame(batch)
	if err != nil {
		t.Fatalf("EncodeQuerySubscriptionProgressFrame() error = %v", err)
	}
	if len(payload) >= 64 {
		t.Fatalf("progress frame length = %d, want a compact frame", len(payload))
	}
	decoded, err := hatSql.DecodeQuerySubscriptionProgressFrame(payload)
	if err != nil {
		t.Fatalf("DecodeQuerySubscriptionProgressFrame() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, batch) {
		t.Fatalf("decoded progress frame = %#v, want %#v", decoded, batch)
	}
	second, err := hatSql.EncodeQuerySubscriptionProgressFrame(batch)
	if err != nil {
		t.Fatalf("second EncodeQuerySubscriptionProgressFrame() error = %v", err)
	}
	if string(second) != string(payload) {
		t.Fatal("progress frame encoding is not deterministic")
	}
}

func TestQuerySubscriptionProgressFrameRejectsDataAndCorruption(t *testing.T) {
	dataBatch := hatSql.QuerySubscriptionDeltaBatch{
		ID:       1,
		Revision: 2,
		Frontier: 3,
		Deltas: []hatSql.QuerySubscriptionDelta{{
			Row:  hatSql.Row{"id": int64(1)},
			Diff: 1,
		}},
	}
	if _, err := hatSql.EncodeQuerySubscriptionProgressFrame(dataBatch); !errors.Is(err, hatSql.ErrQuerySubscriptionProgressFrameInvalid) {
		t.Fatalf("data batch error = %v, want ErrQuerySubscriptionProgressFrameInvalid", err)
	}

	valid, err := hatSql.EncodeQuerySubscriptionProgressFrame(hatSql.QuerySubscriptionDeltaBatch{Frontier: 3, Progress: true})
	if err != nil {
		t.Fatalf("EncodeQuerySubscriptionProgressFrame() error = %v", err)
	}
	corrupt := append([]byte(nil), valid...)
	corrupt[len(corrupt)-1] ^= 0x80
	if _, err := hatSql.DecodeQuerySubscriptionProgressFrame(corrupt); !errors.Is(err, hatSql.ErrQuerySubscriptionProgressFrameCorrupt) {
		t.Fatalf("corrupt frame error = %v, want ErrQuerySubscriptionProgressFrameCorrupt", err)
	}
	trailing := append(append([]byte(nil), valid...), 0)
	if _, err := hatSql.DecodeQuerySubscriptionProgressFrame(trailing); !errors.Is(err, hatSql.ErrQuerySubscriptionProgressFrameCorrupt) {
		t.Fatalf("trailing frame error = %v, want ErrQuerySubscriptionProgressFrameCorrupt", err)
	}
}

func TestQuerySubscriptionProgressFramePayloadSize(t *testing.T) {
	batch := hatSql.QuerySubscriptionDeltaBatch{ID: 7, Revision: 42, Frontier: 9001, Progress: true, Complete: true}
	compact, err := hatSql.EncodeQuerySubscriptionProgressFrame(batch)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("qpf1 payload bytes=%d", len(compact))
	t.Logf("json payload bytes=%d", len(legacy))
}
