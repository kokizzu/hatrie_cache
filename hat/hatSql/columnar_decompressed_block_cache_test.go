package hatSql

import (
	"reflect"
	"testing"
)

func TestColumnarDecompressedBlockCacheAdmissionAndRoundTrip(t *testing.T) {
	values := []interface{}{int64(7), nil, int64(9), int64(11)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackCompressedColumns()
	batch.decompressedBlockCache = newColumnarDecompressedBlockCache(batch, 1<<20, 2, 2)

	if got, valid := batch.Value("value", 0); !valid || !reflect.DeepEqual(got, values[0]) {
		t.Fatalf("first lookup = %#v, %v; want %#v, true", got, valid, values[0])
	}
	if got := len(batch.decompressedBlockCache.loadedBlocks()); got != 0 {
		t.Fatalf("loaded blocks after admission miss = %d, want 0", got)
	}
	if got, valid := batch.Value("value", 0); !valid || !reflect.DeepEqual(got, values[0]) {
		t.Fatalf("admitted lookup = %#v, %v; want %#v, true", got, valid, values[0])
	}
	if got := len(batch.decompressedBlockCache.loadedBlocks()); got != 1 {
		t.Fatalf("loaded blocks after admission = %d, want 1", got)
	}
	if got, valid := batch.Value("value", 1); !valid || got != nil {
		t.Fatalf("cached NULL lookup = %#v, %v; want nil, true", got, valid)
	}
}

func TestColumnarDecompressedBlockCacheRespectsByteBudget(t *testing.T) {
	values := []interface{}{int64(1), int64(2), int64(3), int64(4)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackCompressedColumns()
	blockBytes := 64 + 2*16 + 1
	batch.decompressedBlockCache = newColumnarDecompressedBlockCache(batch, blockBytes, 2, 1)

	for _, row := range []int{0, 2} {
		if got, valid := batch.Value("value", row); !valid || !reflect.DeepEqual(got, values[row]) {
			t.Fatalf("lookup row %d = %#v, %v; want %#v, true", row, got, valid, values[row])
		}
	}
	if got := len(batch.decompressedBlockCache.loadedBlocks()); got != 1 {
		t.Fatalf("loaded blocks = %d, want one block under budget", got)
	}
	if got := batch.decompressedBlockCache.bytes; got > blockBytes {
		t.Fatalf("cache bytes = %d, want <= %d", got, blockBytes)
	}
}

func TestColumnarDecompressedBlockCacheConcurrentLookup(t *testing.T) {
	values := make([]interface{}, 1024)
	for row := range values {
		values[row] = int64(row)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackCompressedColumns()
	batch.decompressedBlockCache = newColumnarDecompressedBlockCache(batch, 1<<20, 64, 1)

	const workers = 8
	done := make(chan struct{}, workers)
	for worker := 0; worker < workers; worker++ {
		go func(offset int) {
			for iteration := 0; iteration < 32; iteration++ {
				for row := offset; row < len(values); row += workers {
					got, valid := batch.Value("value", row)
					if !valid || !reflect.DeepEqual(got, values[row]) {
						t.Errorf("lookup row %d = %#v, %v; want %#v, true", row, got, valid, values[row])
						return
					}
				}
			}
			done <- struct{}{}
		}(worker)
	}
	for worker := 0; worker < workers; worker++ {
		<-done
	}
}

func TestColumnarDecompressedBlockCacheHandlesLargeBlockRows(t *testing.T) {
	values := []interface{}{int64(1), int64(2), int64(3)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackCompressedColumns()
	maxInt := int(^uint(0) >> 1)
	batch.decompressedBlockCache = newColumnarDecompressedBlockCache(batch, 1<<20, maxInt, 1)
	if got := len(batch.decompressedBlockCache.columns["value"].blocks); got != 1 {
		t.Fatalf("large block slot count = %d, want 1", got)
	}

	for row, want := range values {
		got, valid := batch.Value("value", row)
		if !valid || !reflect.DeepEqual(got, want) {
			t.Fatalf("lookup row %d = %#v, %v; want %#v, true", row, got, valid, want)
		}
	}
}
