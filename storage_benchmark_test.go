package hatriecache

import "testing"

const (
	benchmarkStorageItems = 1 << 14
	benchmarkPayloadSize  = 64
)

var (
	benchmarkIndexSink int32
	benchmarkBytesSink []byte
	benchmarkBoolSink  bool
)

func BenchmarkRawBytesSliceAdd(b *testing.B) {
	payloads := benchmarkPayloads(benchmarkStorageItems, benchmarkPayloadSize)
	mask := benchmarkStorageItems - 1
	store := CreateBytesStorage()

	b.ReportAllocs()
	b.SetBytes(benchmarkPayloadSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if len(store.array) == benchmarkStorageItems {
			store = CreateBytesStorage()
		}
		benchmarkIndexSink = store.Add(payloads[i&mask])
	}
}

func BenchmarkRawBytesMapSet(b *testing.B) {
	payloads := benchmarkPayloads(benchmarkStorageItems, benchmarkPayloadSize)
	mask := benchmarkStorageItems - 1
	values := make(map[int32][]byte, benchmarkStorageItems)
	var idx int32

	b.ReportAllocs()
	b.SetBytes(benchmarkPayloadSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if len(values) == benchmarkStorageItems {
			values = make(map[int32][]byte, benchmarkStorageItems)
			idx = 0
		}
		values[idx] = cloneBytes(payloads[i&mask])
		benchmarkIndexSink = idx
		idx++
	}
}

func BenchmarkRawBytesSliceLookup(b *testing.B) {
	store, _ := benchmarkRawBytesStores()
	mask := benchmarkStorageItems - 1

	b.ReportAllocs()
	b.SetBytes(benchmarkPayloadSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := int32((i * 2654435761) & mask)
		benchmarkBytesSink = store.array[idx]
	}
}

func BenchmarkRawBytesMapLookup(b *testing.B) {
	_, values := benchmarkRawBytesStores()
	mask := benchmarkStorageItems - 1

	b.ReportAllocs()
	b.SetBytes(benchmarkPayloadSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := int32((i * 2654435761) & mask)
		benchmarkBytesSink = values[idx]
	}
}

func BenchmarkRawBytesSliceReplace(b *testing.B) {
	store, _ := benchmarkRawBytesStores()
	payloads := benchmarkPayloads(benchmarkStorageItems, benchmarkPayloadSize)
	mask := benchmarkStorageItems - 1

	b.ReportAllocs()
	b.SetBytes(benchmarkPayloadSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := int32((i * 2654435761) & mask)
		store.Put(idx, payloads[(i+1)&mask])
	}
}

func BenchmarkRawBytesMapReplace(b *testing.B) {
	_, values := benchmarkRawBytesStores()
	payloads := benchmarkPayloads(benchmarkStorageItems, benchmarkPayloadSize)
	mask := benchmarkStorageItems - 1

	b.ReportAllocs()
	b.SetBytes(benchmarkPayloadSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := int32((i * 2654435761) & mask)
		values[idx] = cloneBytes(payloads[(i+1)&mask])
	}
}

func BenchmarkRawBytesSliceDeleteReuse(b *testing.B) {
	store, _ := benchmarkRawBytesStores()
	payloads := benchmarkPayloads(benchmarkStorageItems, benchmarkPayloadSize)
	mask := benchmarkStorageItems - 1

	b.ReportAllocs()
	b.SetBytes(benchmarkPayloadSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := int32(i & mask)
		store.Del(idx)
		benchmarkIndexSink = store.Add(payloads[i&mask])
	}
}

func BenchmarkRawBytesMapDeleteSet(b *testing.B) {
	_, values := benchmarkRawBytesStores()
	payloads := benchmarkPayloads(benchmarkStorageItems, benchmarkPayloadSize)
	mask := benchmarkStorageItems - 1

	b.ReportAllocs()
	b.SetBytes(benchmarkPayloadSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := int32(i & mask)
		delete(values, idx)
		values[idx] = cloneBytes(payloads[i&mask])
	}
}

func BenchmarkBloomFilterAddKey(b *testing.B) {
	payloads := benchmarkPayloads(benchmarkStorageItems, 16)
	mask := benchmarkStorageItems - 1
	filter := newDefaultBloomFilterData()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i > 0 && i&mask == 0 {
			filter = newDefaultBloomFilterData()
		}
		benchmarkBoolSink = filter.addKey(payloads[i&mask])
	}
}

func BenchmarkBloomFilterContainsKey(b *testing.B) {
	payloads := benchmarkPayloads(benchmarkStorageItems, 16)
	mask := benchmarkStorageItems - 1
	filter := newDefaultBloomFilterData()
	for _, payload := range payloads {
		filter.addKey(payload)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchmarkBoolSink = filter.containsKey(payloads[(i*2654435761)&mask])
	}
}

func BenchmarkRoaringBitmapAdd(b *testing.B) {
	mask := benchmarkStorageItems - 1
	bitmap := newRoaringBitmapData()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i > 0 && i&mask == 0 {
			bitmap = newRoaringBitmapData()
		}
		benchmarkBoolSink = bitmap.Add(uint32(i & mask))
	}
}

func BenchmarkRoaringBitmapContains(b *testing.B) {
	mask := benchmarkStorageItems - 1
	bitmap := newRoaringBitmapData()
	for i := 0; i < benchmarkStorageItems; i++ {
		bitmap.Add(uint32(i))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchmarkBoolSink = bitmap.Contains(uint32((i * 2654435761) & mask))
	}
}

func BenchmarkSparseBitsetAdd(b *testing.B) {
	mask := benchmarkStorageItems - 1
	bitset := newSparseBitsetData()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i > 0 && i&mask == 0 {
			bitset = newSparseBitsetData()
		}
		benchmarkBoolSink = bitset.Add(benchmarkSparseBitsetValue(i & mask))
	}
}

func BenchmarkSparseBitsetContains(b *testing.B) {
	mask := benchmarkStorageItems - 1
	bitset := newSparseBitsetData()
	for i := 0; i < benchmarkStorageItems; i++ {
		bitset.Add(benchmarkSparseBitsetValue(i))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchmarkBoolSink = bitset.Contains(benchmarkSparseBitsetValue((i * 2654435761) & mask))
	}
}

func benchmarkRawBytesStores() (*BytesStorage, map[int32][]byte) {
	payloads := benchmarkPayloads(benchmarkStorageItems, benchmarkPayloadSize)
	store := CreateBytesStorage()
	values := make(map[int32][]byte, benchmarkStorageItems)

	for i, payload := range payloads {
		idx := int32(i)
		store.Append(payload)
		values[idx] = cloneBytes(payload)
	}

	return store, values
}

func benchmarkSparseBitsetValue(idx int) uint64 {
	value := uint64(idx)
	return (value << 32) | value
}

func benchmarkPayloads(count, size int) [][]byte {
	payloads := make([][]byte, count)
	for i := 0; i < count; i++ {
		payload := make([]byte, size)
		for j := 0; j < size; j++ {
			payload[j] = byte(i + j)
		}
		payloads[i] = payload
	}
	return payloads
}
