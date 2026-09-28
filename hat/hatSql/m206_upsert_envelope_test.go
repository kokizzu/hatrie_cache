package hatSql_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM206NormalizeUpsertEnvelope(t *testing.T) {
	row := hatSql.Row{"name": "Ada"}
	change, err := hatSql.NormalizeUpsertEnvelope(hatSql.UpsertEnvelope{
		Sequence: 7,
		Key:      " customer-7 ",
		Row:      row,
	})
	if err != nil {
		t.Fatalf("NormalizeUpsertEnvelope() error = %v", err)
	}
	if change.Sequence != 7 || change.Key != "customer-7" || change.Deleted || change.Row["name"] != "Ada" {
		t.Fatalf("normalized change = %#v", change)
	}

	row["name"] = "Grace"
	if change.Row["name"] != "Grace" {
		t.Fatal("normalized row was copied despite the borrowed-row contract")
	}
}

func TestM206NormalizeUpsertTombstone(t *testing.T) {
	change, err := hatSql.NormalizeUpsertEnvelope(hatSql.UpsertEnvelope{
		Sequence: 9,
		Key:      "customer-9",
		Deleted:  true,
	})
	if err != nil {
		t.Fatalf("NormalizeUpsertEnvelope(tombstone) error = %v", err)
	}
	if !change.Deleted || change.Row != nil || change.Key != "customer-9" {
		t.Fatalf("tombstone change = %#v", change)
	}
}

func TestM206DecodeUpsertEnvelopeJSON(t *testing.T) {
	change, err := hatSql.DecodeUpsertEnvelopeJSON([]byte(`{"sequence":11,"key":"customer-11","row":{"active":true}}`))
	if err != nil {
		t.Fatalf("DecodeUpsertEnvelopeJSON() error = %v", err)
	}
	if change.Sequence != 11 || change.Key != "customer-11" || change.Deleted || change.Row["active"] != true {
		t.Fatalf("decoded change = %#v", change)
	}

	tombstone, err := hatSql.DecodeUpsertEnvelopeJSON([]byte(`{"key":"customer-12","deleted":true,"row":null}`))
	if err != nil {
		t.Fatalf("DecodeUpsertEnvelopeJSON(tombstone) error = %v", err)
	}
	if !tombstone.Deleted || tombstone.Row != nil {
		t.Fatalf("decoded tombstone = %#v", tombstone)
	}
}

func TestM206RejectsInvalidUpsertEnvelopes(t *testing.T) {
	tests := []struct {
		name     string
		envelope hatSql.UpsertEnvelope
	}{{
		name:     "missing key",
		envelope: hatSql.UpsertEnvelope{Row: hatSql.Row{"v": 1}},
	}, {
		name:     "missing row",
		envelope: hatSql.UpsertEnvelope{Key: "k"},
	}, {
		name:     "tombstone row",
		envelope: hatSql.UpsertEnvelope{Key: "k", Deleted: true, Row: hatSql.Row{"v": 1}},
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := hatSql.NormalizeUpsertEnvelope(test.envelope); !errors.Is(err, hatSql.ErrUpsertEnvelopeInvalid) {
				t.Fatalf("error = %v, want ErrUpsertEnvelopeInvalid", err)
			}
		})
	}

	if _, err := hatSql.DecodeUpsertEnvelopeJSON([]byte(`{"key":"k","row":`)); !errors.Is(err, hatSql.ErrUpsertEnvelopeInvalid) {
		t.Fatalf("malformed JSON error = %v, want ErrUpsertEnvelopeInvalid", err)
	}
}
