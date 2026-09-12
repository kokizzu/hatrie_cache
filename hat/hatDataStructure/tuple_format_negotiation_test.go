package hatDataStructure_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestTupleFormatNegotiationSelectsHighestExactCompatibleVersion(t *testing.T) {
	fields := []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
		{Name: "region", Type: hatDataStructure.TupleFieldString},
	}
	v1 := newNegotiationTupleFormat(t, 1, fields)
	v2 := newNegotiationTupleFormat(t, 2, fields)
	local := []hatDataStructure.TupleFormatCapability{
		v1.MustCapability("orders"),
		v2.MustCapability("orders"),
	}
	remote := []hatDataStructure.TupleFormatCapability{
		v1.MustCapability("orders"),
		v2.MustCapability("orders"),
	}

	chosen, err := hatDataStructure.NegotiateTupleFormat(local, remote)
	if err != nil {
		t.Fatalf("NegotiateTupleFormat() error = %v", err)
	}
	if chosen.Name != "orders" || chosen.Version != 2 || chosen.Fingerprint != local[1].Fingerprint {
		t.Fatalf("chosen capability = %#v, want orders v2 with local fingerprint", chosen)
	}
}

func TestTupleFormatNegotiationRequiresMatchingShapeAndName(t *testing.T) {
	localFormat := newNegotiationTupleFormat(t, 3, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
	})
	sameShapeFormat := newNegotiationTupleFormat(t, 3, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
	})
	changedFormat := newNegotiationTupleFormat(t, 3, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldString},
	})
	local := []hatDataStructure.TupleFormatCapability{localFormat.MustCapability("orders")}

	for name, remote := range map[string][]hatDataStructure.TupleFormatCapability{
		"different name":  {sameShapeFormat.MustCapability("users")},
		"different shape": {changedFormat.MustCapability("orders")},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := hatDataStructure.NegotiateTupleFormat(local, remote); !errors.Is(err, hatDataStructure.ErrTupleFormatNoCompatibleVersion) {
				t.Fatalf("NegotiateTupleFormat() error = %v, want %v", err, hatDataStructure.ErrTupleFormatNoCompatibleVersion)
			}
		})
	}
}

func TestTupleFormatCapabilitiesRoundTripDeterministically(t *testing.T) {
	fields := []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldInt64},
		{Name: "active", Type: hatDataStructure.TupleFieldBool, Nullable: true},
	}
	v1 := newNegotiationTupleFormat(t, 1, fields)
	v2 := newNegotiationTupleFormat(t, 2, fields)
	v3 := newNegotiationTupleFormat(t, 3, fields)
	capabilities := []hatDataStructure.TupleFormatCapability{
		v3.MustCapability("orders"),
		v2.MustCapability("orders"),
		v1.MustCapability("orders"),
	}

	encoded, err := hatDataStructure.MarshalTupleFormatCapabilities(capabilities)
	if err != nil {
		t.Fatalf("MarshalTupleFormatCapabilities() error = %v", err)
	}
	decoded, err := hatDataStructure.UnmarshalTupleFormatCapabilities(encoded)
	if err != nil {
		t.Fatalf("UnmarshalTupleFormatCapabilities() error = %v", err)
	}
	if len(decoded) != len(capabilities) {
		t.Fatalf("decoded capability count = %d, want %d", len(decoded), len(capabilities))
	}
	if decoded[0].Version != 1 || decoded[1].Version != 2 || decoded[2].Version != 3 {
		t.Fatalf("decoded capabilities = %#v, want canonical version order", decoded)
	}
	reencoded, err := hatDataStructure.MarshalTupleFormatCapabilities(decoded)
	if err != nil {
		t.Fatalf("MarshalTupleFormatCapabilities(decoded) error = %v", err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatalf("capability encoding is not deterministic: %x != %x", encoded, reencoded)
	}
	jsonEncoded, err := json.Marshal(capabilities)
	if err != nil {
		t.Fatalf("json.Marshal(capabilities) error = %v", err)
	}
	t.Logf("capability payload sizes: binary=%d JSON=%d", len(encoded), len(jsonEncoded))
}

func TestTupleFormatCapabilitiesRejectMalformedWireAndInvalidInputs(t *testing.T) {
	format := newNegotiationTupleFormat(t, 1, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
	})
	capability := format.MustCapability("orders")
	encoded, err := hatDataStructure.MarshalTupleFormatCapabilities([]hatDataStructure.TupleFormatCapability{capability})
	if err != nil {
		t.Fatalf("MarshalTupleFormatCapabilities() error = %v", err)
	}

	malformed := append(append([]byte(nil), encoded...), 0)
	if _, err := hatDataStructure.UnmarshalTupleFormatCapabilities(malformed); !errors.Is(err, hatDataStructure.ErrTupleFormatCapabilityWire) {
		t.Fatalf("trailing bytes error = %v, want %v", err, hatDataStructure.ErrTupleFormatCapabilityWire)
	}
	unsupportedVersion := append([]byte(nil), encoded...)
	unsupportedVersion[4]++
	if _, err := hatDataStructure.UnmarshalTupleFormatCapabilities(unsupportedVersion); !errors.Is(err, hatDataStructure.ErrTupleFormatCapabilityWire) {
		t.Fatalf("unsupported wire version error = %v, want %v", err, hatDataStructure.ErrTupleFormatCapabilityWire)
	}
	if _, err := hatDataStructure.MarshalTupleFormatCapabilities([]hatDataStructure.TupleFormatCapability{{Name: "orders", Version: 1}}); !errors.Is(err, hatDataStructure.ErrTupleFormatCapabilityInvalid) {
		t.Fatalf("zero fingerprint error = %v, want %v", err, hatDataStructure.ErrTupleFormatCapabilityInvalid)
	}
	if _, err := hatDataStructure.NegotiateTupleFormat(nil, nil); !errors.Is(err, hatDataStructure.ErrTupleFormatNoCompatibleVersion) {
		t.Fatalf("empty negotiation error = %v, want %v", err, hatDataStructure.ErrTupleFormatNoCompatibleVersion)
	}
}

func BenchmarkTupleFormatCapabilitiesMarshal(b *testing.B) {
	capabilities := benchmarkTupleFormatCapabilities(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := hatDataStructure.MarshalTupleFormatCapabilities(capabilities)
		if err != nil {
			b.Fatal(err)
		}
		tupleFormatCapabilityBenchmarkSink = encoded
	}
}

func BenchmarkJSONTupleFormatCapabilitiesMarshal(b *testing.B) {
	capabilities := benchmarkTupleFormatCapabilities(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := json.Marshal(capabilities)
		if err != nil {
			b.Fatal(err)
		}
		tupleFormatCapabilityBenchmarkSink = encoded
	}
}

func BenchmarkTupleFormatCapabilitiesUnmarshal(b *testing.B) {
	capabilities := benchmarkTupleFormatCapabilities(b)
	encoded, err := hatDataStructure.MarshalTupleFormatCapabilities(capabilities)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		decoded, err := hatDataStructure.UnmarshalTupleFormatCapabilities(encoded)
		if err != nil {
			b.Fatal(err)
		}
		tupleFormatCapabilityBenchmarkSink = decoded
	}
}

func BenchmarkJSONTupleFormatCapabilitiesUnmarshal(b *testing.B) {
	capabilities := benchmarkTupleFormatCapabilities(b)
	encoded, err := json.Marshal(capabilities)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var decoded []hatDataStructure.TupleFormatCapability
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
		tupleFormatCapabilityBenchmarkSink = decoded
	}
}

func BenchmarkTupleFormatNegotiation(b *testing.B) {
	local := benchmarkTupleFormatCapabilities(b)
	remote := append([]hatDataStructure.TupleFormatCapability(nil), local...)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		chosen, err := hatDataStructure.NegotiateTupleFormat(local, remote)
		if err != nil {
			b.Fatal(err)
		}
		tupleFormatCapabilityBenchmarkSink = chosen
	}
}

type negotiationTupleFormat interface {
	MustCapability(string) hatDataStructure.TupleFormatCapability
}

func newNegotiationTupleFormat(t *testing.T, version uint64, fields []hatDataStructure.TupleFieldSpec) negotiationTupleFormat {
	t.Helper()
	format, err := hatDataStructure.NewTupleFormat(version, fields)
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	return tupleFormatCapabilityAdapter{format: format}
}

type tupleFormatCapabilityAdapter struct {
	format hatDataStructure.TupleFormat
}

func (adapter tupleFormatCapabilityAdapter) MustCapability(name string) hatDataStructure.TupleFormatCapability {
	capability, err := adapter.format.Capability(name)
	if err != nil {
		panic(err)
	}
	return capability
}

func benchmarkTupleFormatCapabilities(t testing.TB) []hatDataStructure.TupleFormatCapability {
	t.Helper()
	format, err := hatDataStructure.NewTupleFormat(7, []hatDataStructure.TupleFieldSpec{
		{Name: "id", Type: hatDataStructure.TupleFieldUint64},
		{Name: "region", Type: hatDataStructure.TupleFieldString},
		{Name: "active", Type: hatDataStructure.TupleFieldBool, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	capability, err := format.Capability("orders")
	if err != nil {
		t.Fatalf("Capability() error = %v", err)
	}
	capabilityV6 := capability
	capabilityV6.Version = 6
	capabilityV8 := capability
	capabilityV8.Version = 8
	return []hatDataStructure.TupleFormatCapability{capabilityV6, capability, capabilityV8}
}

var tupleFormatCapabilityBenchmarkSink any
