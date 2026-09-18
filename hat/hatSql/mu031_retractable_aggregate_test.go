package hatSql_test

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type mu031RetractableSum struct {
	total int64
}

func (state *mu031RetractableSum) Add(value interface{}) error {
	number, ok := value.(int64)
	if !ok {
		return errors.New("mu031 sum expects int64")
	}
	state.total += number
	return nil
}

func (state *mu031RetractableSum) Retract(value interface{}) error {
	number, ok := value.(int64)
	if !ok {
		return errors.New("mu031 sum expects int64")
	}
	state.total -= number
	return nil
}

func (state *mu031RetractableSum) Merge(other hatSql.SQLAggregateState) error {
	source, ok := other.(*mu031RetractableSum)
	if !ok {
		return errors.New("mu031 sum state type mismatch")
	}
	state.total += source.total
	return nil
}

func (state *mu031RetractableSum) Finalize() (interface{}, error) {
	return state.total, nil
}

type mu031SerializableSum struct {
	mu031RetractableSum
}

func (state *mu031SerializableSum) MarshalBinary() ([]byte, error) {
	encoded := make([]byte, 8)
	binary.LittleEndian.PutUint64(encoded, uint64(state.total))
	return encoded, nil
}

func (state *mu031SerializableSum) UnmarshalBinary(encoded []byte) error {
	if len(encoded) != 8 {
		return errors.New("mu031 serialized sum has invalid length")
	}
	state.total = int64(binary.LittleEndian.Uint64(encoded))
	return nil
}

func TestMU031AggregateRegistryDiscoversRetractableState(t *testing.T) {
	registry := hatSql.NewSQLAggregateCombinatorRegistry()
	combinator, err := hatSql.NewSQLAggregateCombinator("retract_sum", func() hatSql.SQLAggregateState {
		return &mu031RetractableSum{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(combinator); err != nil {
		t.Fatal(err)
	}
	state, err := registry.NewRetractableState("RETRACT_SUM")
	if err != nil {
		t.Fatalf("NewRetractableState() error = %v", err)
	}
	if err := state.Add(int64(9)); err != nil {
		t.Fatal(err)
	}
	if err := state.Add(int64(4)); err != nil {
		t.Fatal(err)
	}
	if err := state.Retract(int64(4)); err != nil {
		t.Fatal(err)
	}
	result, err := state.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if result != int64(9) {
		t.Fatalf("Finalize() = %v, want 9", result)
	}
}

func TestMU031AggregateCombinatorDiscoversCapabilities(t *testing.T) {
	retractableCombinator, err := hatSql.NewSQLAggregateCombinator("retract_sum", func() hatSql.SQLAggregateState {
		return &mu031RetractableSum{}
	})
	if err != nil {
		t.Fatal(err)
	}
	retractable, err := retractableCombinator.NewRetractableState()
	if err != nil {
		t.Fatalf("NewRetractableState() error = %v", err)
	}
	if err := retractable.Add(int64(6)); err != nil {
		t.Fatal(err)
	}
	result, err := retractable.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if result != int64(6) {
		t.Fatalf("Finalize() = %v, want 6", result)
	}

	serializableCombinator, err := hatSql.NewSQLAggregateCombinator("serial_sum", func() hatSql.SQLAggregateState {
		return &mu031SerializableSum{}
	})
	if err != nil {
		t.Fatal(err)
	}
	serializable, err := serializableCombinator.NewSerializableState()
	if err != nil {
		t.Fatalf("NewSerializableState() error = %v", err)
	}
	if err := serializable.Add(int64(8)); err != nil {
		t.Fatal(err)
	}
	encoded, err := serializable.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 {
		t.Fatal("MarshalBinary() returned an empty snapshot")
	}
}

func TestMU031AggregateRegistryRejectsMissingCapabilities(t *testing.T) {
	registry := hatSql.NewSQLAggregateCombinatorRegistry()
	legacy, err := hatSql.NewSQLAggregateCombinator("legacy", func() hatSql.SQLAggregateState {
		return &mu031LegacySum{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.NewRetractableState("legacy"); !errors.Is(err, hatSql.ErrSQLAggregateCombinatorNotRetractable) {
		t.Fatalf("NewRetractableState() error = %v, want not-retractable", err)
	}
	if _, err := registry.NewSerializableState("legacy"); !errors.Is(err, hatSql.ErrSQLAggregateCombinatorNotSerializable) {
		t.Fatalf("NewSerializableState() error = %v, want not-serializable", err)
	}
}

func TestMU031AggregateRegistryDiscoversSerializableState(t *testing.T) {
	registry := hatSql.NewSQLAggregateCombinatorRegistry()
	combinator, err := hatSql.NewSQLAggregateCombinator("serial_sum", func() hatSql.SQLAggregateState {
		return &mu031SerializableSum{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(combinator); err != nil {
		t.Fatal(err)
	}
	state, err := registry.NewSerializableState("serial_sum")
	if err != nil {
		t.Fatalf("NewSerializableState() error = %v", err)
	}
	if err := state.Add(int64(17)); err != nil {
		t.Fatal(err)
	}
	encoded, err := state.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := registry.NewSerializableState("serial_sum")
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	result, err := restored.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if result != int64(17) {
		t.Fatalf("restored Finalize() = %v, want 17", result)
	}
	if !reflect.DeepEqual(encoded, []byte{17, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("encoded = %v", encoded)
	}
}

type mu031LegacySum struct {
	total int64
}

func (state *mu031LegacySum) Add(value interface{}) error {
	number, ok := value.(int64)
	if !ok {
		return errors.New("mu031 legacy sum expects int64")
	}
	state.total += number
	return nil
}

func (state *mu031LegacySum) Merge(other hatSql.SQLAggregateState) error {
	source, ok := other.(*mu031LegacySum)
	if !ok {
		return errors.New("mu031 legacy state type mismatch")
	}
	state.total += source.total
	return nil
}

func (state *mu031LegacySum) Finalize() (interface{}, error) {
	return state.total, nil
}
