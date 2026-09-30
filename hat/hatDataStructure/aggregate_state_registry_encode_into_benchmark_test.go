package hatDataStructure_test

import (
	"testing"

	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

func BenchmarkAggregateStateRegistryEncodeInto(b *testing.B) {
	registry := hatDataStructure.NewAggregateStateRegistry()
	if err := registry.Register(newRegistrySumCodec(b, 1)); err != nil {
		b.Fatal(err)
	}
	state := registrySumState{Total: 123456}
	wire, err := registry.Encode("sum", 1, state)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, 0, len(wire))
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	for range b.N {
		dst, err = registry.EncodeInto(dst, "sum", 1, state)
		if err != nil {
			b.Fatal(err)
		}
	}
}
