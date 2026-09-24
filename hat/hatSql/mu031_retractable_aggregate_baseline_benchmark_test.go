package hatSql

import "testing"

type mu031BenchmarkSum struct {
	total int64
}

func (state *mu031BenchmarkSum) Add(value interface{}) error {
	state.total += value.(int64)
	return nil
}

func (state *mu031BenchmarkSum) Merge(other SQLAggregateState) error {
	state.total += other.(*mu031BenchmarkSum).total
	return nil
}

func (state *mu031BenchmarkSum) Finalize() (interface{}, error) {
	return state.total, nil
}

func (state *mu031BenchmarkSum) MarshalBinary() ([]byte, error) {
	return []byte{byte(state.total), byte(state.total >> 8), byte(state.total >> 16), byte(state.total >> 24), byte(state.total >> 32), byte(state.total >> 40), byte(state.total >> 48), byte(state.total >> 56)}, nil
}

func (state *mu031BenchmarkSum) UnmarshalBinary(data []byte) error {
	state.total = int64(uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24 | uint64(data[4])<<32 | uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56)
	return nil
}

func BenchmarkMU031AggregateDirectAdd(b *testing.B) {
	state := &mu031BenchmarkSum{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := state.Add(int64(1)); err != nil {
			b.Fatal(err)
		}
	}
}
