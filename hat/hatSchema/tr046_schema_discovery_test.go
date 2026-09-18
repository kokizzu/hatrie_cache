package hatSchema

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestTR046SchemaDiscoveryRoundTripIsCanonicalAndBounded(t *testing.T) {
	schema := schemaCompatibilityFixture(7)
	discovery, err := NewSchemaDiscovery(schema, 3, []string{"drop_nullable_column", "add_nullable_column"})
	if err != nil {
		t.Fatalf("NewSchemaDiscovery() error = %v", err)
	}
	if got, want := discovery.DDLCapabilities, []string{"add_nullable_column", "drop_nullable_column"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DDLCapabilities = %#v, want %#v", got, want)
	}
	wire, err := discovery.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	decoded, err := DecodeSchemaDiscovery(wire)
	if err != nil {
		t.Fatalf("DecodeSchemaDiscovery() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, discovery) {
		t.Fatalf("decoded discovery = %#v, want %#v", decoded, discovery)
	}
	var unmarshaled SchemaDiscovery
	if err := unmarshaled.UnmarshalBinary(wire); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}
	if !reflect.DeepEqual(unmarshaled, discovery) {
		t.Fatalf("unmarshaled discovery = %#v, want %#v", unmarshaled, discovery)
	}
	reencoded, err := decoded.MarshalBinary()
	if err != nil {
		t.Fatalf("decoded.MarshalBinary() error = %v", err)
	}
	if !bytes.Equal(reencoded, wire) {
		t.Fatalf("reencoded bytes = %x, want canonical %x", reencoded, wire)
	}

	jsonWire, err := json.Marshal(discovery)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if len(wire) >= len(jsonWire) {
		t.Fatalf("binary discovery length = %d, JSON length = %d; want binary smaller", len(wire), len(jsonWire))
	}
}

func TestTR046SchemaDiscoveryRejectsMalformedAndNonCanonicalInput(t *testing.T) {
	schema := schemaCompatibilityFixture(1)
	discovery, err := NewSchemaDiscovery(schema, 1, []string{"add_nullable_column"})
	if err != nil {
		t.Fatalf("NewSchemaDiscovery() error = %v", err)
	}
	wire, err := discovery.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "truncated", data: wire[:len(wire)-1]},
		{name: "trailing bytes", data: append(append([]byte(nil), wire...), 0)},
		{name: "bad magic", data: append([]byte("BAD1"), wire[4:]...)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeSchemaDiscovery(test.data); err == nil {
				t.Fatalf("DecodeSchemaDiscovery(%s) error = nil", test.name)
			}
		})
	}

	if _, err := NewSchemaDiscovery(schema, 0, []string{"add_nullable_column"}); err == nil {
		t.Fatal("NewSchemaDiscovery(protocol 0) error = nil")
	}
	if _, err := NewSchemaDiscovery(schema, 1, []string{"drop_nullable_column", "drop_nullable_column"}); err == nil {
		t.Fatal("NewSchemaDiscovery(duplicate capability) error = nil")
	}
	if _, err := (SchemaDiscovery{
		ProtocolVersion:   1,
		SchemaVersion:     schema.Version,
		SchemaFingerprint: schema.Fingerprint(),
		DDLCapabilities:   []string{"z", "a"},
	}).MarshalBinary(); err == nil {
		t.Fatal("MarshalBinary(non-canonical capabilities) error = nil")
	}
}

func TestTR046SchemaDiscoveryCompatibilityReport(t *testing.T) {
	schema := schemaCompatibilityFixture(4)
	local, err := NewSchemaDiscovery(schema, 2, []string{"add_nullable_column", "drop_nullable_column"})
	if err != nil {
		t.Fatalf("NewSchemaDiscovery(local) error = %v", err)
	}
	peer, err := NewSchemaDiscovery(schema, 2, []string{"add_nullable_column"})
	if err != nil {
		t.Fatalf("NewSchemaDiscovery(peer) error = %v", err)
	}
	report, err := CompareSchemaDiscovery(local, peer, []string{"add_nullable_column", "drop_nullable_column"})
	if err != nil {
		t.Fatalf("CompareSchemaDiscovery() error = %v", err)
	}
	if report.Compatible || !report.ProtocolMatch || !report.SchemaMatch || report.NeedsSchemaTransfer {
		t.Fatalf("report = %#v, want capability mismatch without schema transfer", report)
	}
	if !reflect.DeepEqual(report.MissingDDLCapabilities, []string{"drop_nullable_column"}) {
		t.Fatalf("MissingDDLCapabilities = %#v", report.MissingDDLCapabilities)
	}

	next := schema.Clone()
	next.Version++
	next.Sources["users"] = Source{
		Name:    "users",
		Columns: []Column{{Name: "id", Type: TypeInteger, NotNull: true}, {Name: "name", Type: TypeText}, {Name: "email", Type: TypeText}},
	}
	peer, err = NewSchemaDiscovery(next, 2, []string{"add_nullable_column", "drop_nullable_column"})
	if err != nil {
		t.Fatalf("NewSchemaDiscovery(next peer) error = %v", err)
	}
	report, err = CompareSchemaDiscovery(local, peer, nil)
	if err != nil {
		t.Fatalf("CompareSchemaDiscovery(schema mismatch) error = %v", err)
	}
	if report.Compatible || !report.ProtocolMatch || report.SchemaMatch || !report.NeedsSchemaTransfer {
		t.Fatalf("schema mismatch report = %#v", report)
	}
}

func BenchmarkTR046SchemaDiscoveryBinary(b *testing.B) {
	discovery, err := NewSchemaDiscovery(schemaCompatibilityFixture(7), 3, []string{"add_nullable_column", "drop_nullable_column", "relax_not_null"})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for range b.N {
		wire, err := discovery.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := DecodeSchemaDiscovery(wire); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR046SchemaDiscoveryJSONMarshal(b *testing.B) {
	discovery := benchmarkTR046SchemaDiscoveryFixture(b)
	b.ReportAllocs()
	for range b.N {
		if _, err := json.Marshal(discovery); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR046SchemaDiscoveryBinaryMarshal(b *testing.B) {
	discovery := benchmarkTR046SchemaDiscoveryFixture(b)
	b.ReportAllocs()
	for range b.N {
		if _, err := discovery.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR046SchemaDiscoveryJSONUnmarshal(b *testing.B) {
	discovery := benchmarkTR046SchemaDiscoveryFixture(b)
	wire, err := json.Marshal(discovery)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(wire)))
	b.ReportAllocs()
	for range b.N {
		var decoded SchemaDiscovery
		if err := json.Unmarshal(wire, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTR046SchemaDiscoveryBinaryUnmarshal(b *testing.B) {
	discovery := benchmarkTR046SchemaDiscoveryFixture(b)
	wire, err := discovery.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(wire)))
	b.ReportAllocs()
	for range b.N {
		if _, err := DecodeSchemaDiscovery(wire); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkTR046SchemaDiscoveryFixture(b *testing.B) SchemaDiscovery {
	b.Helper()
	discovery, err := NewSchemaDiscovery(schemaCompatibilityFixture(7), 3, []string{"add_nullable_column", "drop_nullable_column", "relax_not_null"})
	if err != nil {
		b.Fatal(err)
	}
	return discovery
}
