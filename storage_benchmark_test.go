package hatriecache

import "testing"

const (
	benchmarkStorageItems = 1 << 14
	benchmarkPayloadSize  = 64
)

var (
	benchmarkIndexSink int32
	benchmarkBytesSink []byte
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
