package hatSql

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestSQLPublicationPersistenceRoundTrip(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id", "payload", "meta"}, SQLPublicationOptions{MaxHistoryBatches: 8})
	if err != nil {
		t.Fatalf("NewSQLPublication() error = %v", err)
	}
	first := SQLPublicationBatch{
		Revision: 1,
		Frontier: 10,
		Columns:  []string{"id", "payload", "meta"},
		Deltas: []SQLPublicationDelta{{
			Row: Row{
				"id":      int64(7),
				"payload": []byte("open"),
				"meta":    map[string]interface{}{"tags": []interface{}{"hot", int64(2)}, "active": true},
			},
			Diff: 1,
		}},
	}
	second := SQLPublicationBatch{
		Revision: 2,
		Frontier: 20,
		Columns:  []string{"id", "payload", "meta"},
		Progress: true,
		Deltas: []SQLPublicationDelta{{
			Row:  Row{"id": uint64(8), "payload": time.Unix(123, 456).UTC(), "meta": []interface{}{nil, "closed"}},
			Diff: -1,
		}},
	}
	for _, batch := range []SQLPublicationBatch{first, second} {
		if err := publication.Append(batch); err != nil {
			t.Fatalf("Append(%#v) error = %v", batch, err)
		}
	}
	publication.Close()

	encoded, err := publication.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	restored, err := UnmarshalSQLPublication(encoded)
	if err != nil {
		t.Fatalf("UnmarshalSQLPublication() error = %v", err)
	}
	if got, want := restored.Snapshot(), publication.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored Snapshot() = %#v, want %#v", got, want)
	}

	subscription, err := restored.Subscribe(context.Background(), SQLPublicationCheckpoint{})
	if err != nil {
		t.Fatalf("restored Subscribe() error = %v", err)
	}
	defer subscription.Close()
	for _, want := range []SQLPublicationBatch{first, second} {
		select {
		case got := <-subscription.Updates():
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("replayed batch = %#v, want %#v", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for replayed batch")
		}
	}
	if err := subscription.Err(); err != nil {
		t.Fatalf("restored subscription error = %v, want nil", err)
	}
}

func TestSQLPublicationPersistenceRejectsCorruption(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{})
	if err != nil {
		t.Fatalf("NewSQLPublication() error = %v", err)
	}
	if err := publication.Append(SQLPublicationBatch{Revision: 1, Frontier: 1, Deltas: []SQLPublicationDelta{{Row: Row{"id": int64(1)}, Diff: 1}}}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	encoded, err := publication.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	encoded[len(encoded)/2] ^= 1
	if _, err := UnmarshalSQLPublication(encoded); err == nil {
		t.Fatal("UnmarshalSQLPublication(corrupt) error = nil")
	}
}

func TestSQLPublicationPersistenceRejectsUnsupportedValuesAndOversizedFrames(t *testing.T) {
	publication, err := NewSQLPublication("orders", []string{"id"}, SQLPublicationOptions{})
	if err != nil {
		t.Fatalf("NewSQLPublication() error = %v", err)
	}
	if err := publication.Append(SQLPublicationBatch{Revision: 1, Frontier: 1, Deltas: []SQLPublicationDelta{{Row: Row{"id": struct{}{}}, Diff: 1}}}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, err := publication.MarshalBinary(); !errors.Is(err, ErrSQLPublicationInvalid) {
		t.Fatalf("MarshalBinary(unsupported) error = %v, want ErrSQLPublicationInvalid", err)
	}

	oversized := make([]byte, 16)
	copy(oversized[:4], []byte("HPS1"))
	// The payload length is rejected before checksum or payload allocation.
	binary.BigEndian.PutUint64(oversized[4:12], MaxSQLPublicationSnapshotBytes+1)
	if _, err := UnmarshalSQLPublication(oversized); !errors.Is(err, ErrSQLPublicationLimit) {
		t.Fatalf("UnmarshalSQLPublication(oversized) error = %v, want ErrSQLPublicationLimit", err)
	}
}
