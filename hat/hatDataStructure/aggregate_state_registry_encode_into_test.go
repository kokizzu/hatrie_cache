package hatDataStructure_test

import (
	"bytes"
	"errors"
	"testing"

	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

func TestAggregateStateRegistryEncodeIntoReusesDestinationAndMatchesEncode(t *testing.T) {
	registry := hatDataStructure.NewAggregateStateRegistry()
	if err := registry.Register(newRegistrySumCodec(t, 7)); err != nil {
		t.Fatal(err)
	}
	state := registrySumState{Total: 42}
	want, err := registry.Encode("sum", 7, state)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	dst := make([]byte, len(want), len(want)+32)
	for index := range dst {
		dst[index] = 0xa5
	}
	backing := &dst[:cap(dst)][0]
	got, err := registry.EncodeInto(dst, "sum", 7, state)
	if err != nil {
		t.Fatalf("EncodeInto() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeInto() = %x, want %x", got, want)
	}
	if len(got) != len(want) {
		t.Fatalf("EncodeInto() length = %d, want %d", len(got), len(want))
	}
	if &got[0] != backing {
		t.Fatal("EncodeInto() did not reuse destination backing storage")
	}
}

func TestAggregateStateRegistryEncodeIntoPreservesCodecErrors(t *testing.T) {
	registry := hatDataStructure.NewAggregateStateRegistry()
	codec, err := hatDataStructure.NewAggregateStateCodec(
		"failing",
		1,
		func(interface{}) ([]byte, error) { return nil, errors.New("encode rejected") },
		func([]byte) (interface{}, error) { return nil, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(codec); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.EncodeInto(make([]byte, 0, 64), "failing", 1, struct{}{}); !errors.Is(err, hatDataStructure.ErrAggregateStateCodecEncode) {
		t.Fatalf("EncodeInto() error = %v, want codec encode error", err)
	}
}

func TestAggregateStateRegistryEncodeIntoMatchesEncodeAcrossPayloadSizes(t *testing.T) {
	registry := hatDataStructure.NewAggregateStateRegistry()
	codec, err := hatDataStructure.NewAggregateStateCodec(
		"blob",
		1,
		func(value interface{}) ([]byte, error) {
			state, ok := value.(struct{ Size int })
			if !ok {
				return nil, errors.New("blob expects size state")
			}
			payload := make([]byte, state.Size)
			for index := range payload {
				payload[index] = byte(index % 251)
			}
			return payload, nil
		},
		func(payload []byte) (interface{}, error) { return len(payload), nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(codec); err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 1, 127, 128, 255, 1024, 65536} {
		size := size
		t.Run("payload-size", func(t *testing.T) {
			state := struct{ Size int }{Size: size}
			want, err := registry.Encode("blob", 1, state)
			if err != nil {
				t.Fatalf("Encode(%d) error = %v", size, err)
			}
			got, err := registry.EncodeInto(make([]byte, 0, len(want)+16), "blob", 1, state)
			if err != nil {
				t.Fatalf("EncodeInto(%d) error = %v", size, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("EncodeInto(%d) = %x, want %x", size, got, want)
			}
		})
	}
}
