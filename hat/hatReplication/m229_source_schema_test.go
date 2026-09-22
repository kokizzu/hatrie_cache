package hatReplication

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestChangefeedSchemaAdditiveEvolutionAndCompatibility(t *testing.T) {
	v1 := ChangefeedSchema{
		Version: "orders-v1",
		Fields: []ChangefeedSchemaField{
			{Name: "id", Type: "int64"},
			{Name: "status", Type: "string", Nullable: true},
		},
	}
	v2 := ChangefeedSchema{
		Version: "orders-v2",
		Fields: []ChangefeedSchemaField{
			{Name: "id", Type: "int64"},
			{Name: "status", Type: "string", Nullable: true},
			{Name: "created_at", Type: "timestamp", Nullable: true},
		},
	}
	change, err := CheckChangefeedSchemaEvolution(v1, v2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(change.AddedFields, []string{"created_at"}) {
		t.Fatalf("added fields = %#v", change.AddedFields)
	}
	if err := CheckChangefeedSchemaCompatibility(v2, v1); err != nil {
		t.Fatalf("old consumer should accept additive producer: %v", err)
	}
	if err := CheckChangefeedSchemaCompatibility(v1, v2); err != nil {
		t.Fatalf("new consumer should accept missing nullable field: %v", err)
	}
}

func TestChangefeedSchemaRejectsUnsafeEvolution(t *testing.T) {
	base := ChangefeedSchema{
		Version: "v1",
		Fields:  []ChangefeedSchemaField{{Name: "id", Type: "int64"}},
	}
	tests := []struct {
		name string
		next ChangefeedSchema
		want error
	}{
		{name: "type change", next: ChangefeedSchema{Version: "v2", Fields: []ChangefeedSchemaField{{Name: "id", Type: "string"}}}, want: ErrChangefeedSchemaIncompatible},
		{name: "field removal", next: ChangefeedSchema{Version: "v2"}, want: ErrChangefeedSchemaIncompatible},
		{name: "required addition", next: ChangefeedSchema{Version: "v2", Fields: []ChangefeedSchemaField{{Name: "id", Type: "int64"}, {Name: "region", Type: "string"}}}, want: ErrChangefeedSchemaIncompatible},
		{name: "duplicate field", next: ChangefeedSchema{Version: "v2", Fields: []ChangefeedSchemaField{{Name: "id", Type: "int64"}, {Name: "id", Type: "int64", Nullable: true}}}, want: ErrChangefeedSchemaInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := CheckChangefeedSchemaEvolution(base, test.next); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestChangefeedSchemaRegistryKeepsAtomicCurrentVersion(t *testing.T) {
	v1 := ChangefeedSchema{Version: "v1", Fields: []ChangefeedSchemaField{{Name: "id", Type: "int64"}}}
	v2 := ChangefeedSchema{Version: "v2", Fields: []ChangefeedSchemaField{{Name: "id", Type: "int64"}, {Name: "note", Type: "string", HasDefault: true}}}
	registry, err := NewChangefeedSchemaRegistry(v1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Evolve(v2); err != nil {
		t.Fatal(err)
	}
	if got := registry.Current(); !reflect.DeepEqual(got, v2) {
		t.Fatalf("current schema = %#v, want %#v", got, v2)
	}
	if err := registry.Accepts(v1); err != nil {
		t.Fatalf("old consumer should remain compatible: %v", err)
	}
	unsafe := v2
	unsafe.Fields = append([]ChangefeedSchemaField(nil), v2.Fields...)
	unsafe.Fields[0].Type = "string"
	if _, err := registry.Evolve(unsafe); !errors.Is(err, ErrChangefeedSchemaVersionConflict) {
		t.Fatalf("unsafe evolution error = %v", err)
	}
	if got := registry.Current(); !reflect.DeepEqual(got, v2) {
		t.Fatalf("failed evolution changed current schema: %#v", got)
	}
}

func TestSpaceChangefeedTypedSchemaEvolutionPreservesLegacyDefault(t *testing.T) {
	v1 := ChangefeedSchema{Version: "v1", Fields: []ChangefeedSchemaField{{Name: "id", Type: "int64"}}}
	v2 := ChangefeedSchema{Version: "v2", Fields: []ChangefeedSchemaField{{Name: "id", Type: "int64"}, {Name: "region", Type: "string", Nullable: true}}}
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", Schema: &v1, MaxEvents: 4})
	if err != nil {
		t.Fatal(err)
	}
	legacyConsumer, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{
		ExpectedSchemaVersion: "v1",
		Buffer:                1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.EvolveSchema(v2); err != nil {
		t.Fatal(err)
	}
	oldConsumer, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{
		ExpectedSchema: &v1,
		Buffer:         1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := feed.Publish(SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-oldConsumer.Events():
		if event.SchemaVersion != "v2" {
			t.Fatalf("event schema version = %q", event.SchemaVersion)
		}
	default:
		t.Fatal("typed subscriber did not receive evolved event")
	}
	if _, ok := <-legacyConsumer.Events(); ok || !errors.Is(legacyConsumer.Err(), ErrSpaceChangefeedSchemaMismatch) {
		t.Fatalf("legacy subscriber should be fenced on schema evolution: ok=%v err=%v", ok, legacyConsumer.Err())
	}
	if _, err := feed.Subscribe(context.Background(), SpaceChangefeedSubscribeOptions{ExpectedSchema: &ChangefeedSchema{
		Version: "v2",
		Fields:  []ChangefeedSchemaField{{Name: "id", Type: "string"}},
	}}); !errors.Is(err, ErrSpaceChangefeedSchemaMismatch) {
		t.Fatalf("incompatible subscriber schema error = %v", err)
	}
	legacy, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "legacy", SchemaVersion: "v1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.EvolveSchema(v2); !errors.Is(err, ErrSpaceChangefeedSchemaEvolutionUnavailable) {
		t.Fatalf("legacy feed evolution error = %v", err)
	}
}
