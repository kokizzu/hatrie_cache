package hatSql

import (
	"sync"
	"sync/atomic"
)

type columnarDecompressedBlockKey struct {
	field string
	block int
}

type columnarDecompressedBlock struct {
	values   []interface{}
	validity []byte
	bytes    int
	sequence uint64
}

type columnarDecompressedColumn struct {
	blocks []atomic.Pointer[columnarDecompressedBlock]
}

type columnarDecompressedBlockCache struct {
	maxBytes  int
	blockRows int
	minReads  int
	rows      int
	columns   map[string]*columnarDecompressedColumn

	mu       sync.Mutex
	observed map[columnarDecompressedBlockKey]int
	loading  map[columnarDecompressedBlockKey]struct{}
	bytes    int
	sequence uint64
}

func newColumnarDecompressedBlockCache(batch ColumnarBatch, maxBytes, blockRows, minReads int) *columnarDecompressedBlockCache {
	if maxBytes <= 0 {
		maxBytes = typedTableColumnarCacheDefaultDecompressedBlockMaxBytes
	}
	if blockRows <= 0 {
		blockRows = typedTableColumnarCacheDefaultDecompressedBlockRows
	}
	if minReads <= 0 {
		minReads = typedTableColumnarCacheDefaultDecompressedBlockMinReads
	}
	blockCount := 0
	if batch.Rows > 0 {
		blockCount = (batch.Rows-1)/blockRows + 1
	}
	cache := &columnarDecompressedBlockCache{
		maxBytes:  maxBytes,
		blockRows: blockRows,
		minReads:  minReads,
		rows:      batch.Rows,
		columns:   make(map[string]*columnarDecompressedColumn),
		observed:  make(map[columnarDecompressedBlockKey]int),
		loading:   make(map[columnarDecompressedBlockKey]struct{}),
	}
	for field := range batch.Dictionaries {
		cache.columns[field] = &columnarDecompressedColumn{blocks: make([]atomic.Pointer[columnarDecompressedBlock], blockCount)}
	}
	for field := range batch.PackedColumns {
		cache.columns[field] = &columnarDecompressedColumn{blocks: make([]atomic.Pointer[columnarDecompressedBlock], blockCount)}
	}
	for field := range batch.BoolColumns {
		cache.columns[field] = &columnarDecompressedColumn{blocks: make([]atomic.Pointer[columnarDecompressedBlock], blockCount)}
	}
	for field := range batch.NumericColumns {
		cache.columns[field] = &columnarDecompressedColumn{blocks: make([]atomic.Pointer[columnarDecompressedBlock], blockCount)}
	}
	return cache
}

func (cache *columnarDecompressedBlockCache) blockKey(field string, row int) (columnarDecompressedBlockKey, int, bool) {
	if cache == nil || row < 0 || row >= cache.rows || cache.blockRows <= 0 {
		return columnarDecompressedBlockKey{}, 0, false
	}
	column, ok := cache.columns[field]
	if !ok {
		return columnarDecompressedBlockKey{}, 0, false
	}
	block := row / cache.blockRows
	if block < 0 || block >= len(column.blocks) {
		return columnarDecompressedBlockKey{}, 0, false
	}
	return columnarDecompressedBlockKey{field: field, block: block}, row - block*cache.blockRows, true
}

func (cache *columnarDecompressedBlockCache) lookup(key columnarDecompressedBlockKey, offset int) (interface{}, bool, bool) {
	column := cache.columns[key.field]
	if column == nil || key.block < 0 || key.block >= len(column.blocks) {
		return nil, false, false
	}
	block := column.blocks[key.block].Load()
	if block == nil || offset < 0 || offset >= len(block.values) {
		return nil, false, false
	}
	return block.values[offset], block.validAt(offset), true
}

func (cache *columnarDecompressedBlockCache) beginLoad(key columnarDecompressedBlockKey) bool {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	column := cache.columns[key.field]
	if column == nil || key.block < 0 || key.block >= len(column.blocks) || column.blocks[key.block].Load() != nil {
		return false
	}
	if _, found := cache.loading[key]; found {
		return false
	}
	cache.observed[key]++
	if cache.observed[key] < cache.minReads {
		return false
	}
	delete(cache.observed, key)
	cache.loading[key] = struct{}{}
	return true
}

func (cache *columnarDecompressedBlockCache) finishLoad(key columnarDecompressedBlockKey, block columnarDecompressedBlock) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	delete(cache.loading, key)
	column := cache.columns[key.field]
	if column == nil || key.block < 0 || key.block >= len(column.blocks) || block.bytes > cache.maxBytes || column.blocks[key.block].Load() != nil {
		return
	}
	for cache.bytes > cache.maxBytes-block.bytes && cache.loadedBlockCountLocked() > 0 {
		cache.evictOldestLocked()
	}
	if cache.bytes > cache.maxBytes-block.bytes {
		return
	}
	cache.sequence++
	block.sequence = cache.sequence
	column.blocks[key.block].Store(&block)
	cache.bytes += block.bytes
}

func (cache *columnarDecompressedBlockCache) evictOldestLocked() {
	var oldestColumn *columnarDecompressedColumn
	oldestIndex := -1
	var oldest *columnarDecompressedBlock
	for _, column := range cache.columns {
		for index := range column.blocks {
			block := column.blocks[index].Load()
			if block == nil {
				continue
			}
			if oldest == nil || block.sequence < oldest.sequence {
				oldestColumn = column
				oldestIndex = index
				oldest = block
			}
		}
	}
	if oldest == nil {
		return
	}
	oldestColumn.blocks[oldestIndex].Store(nil)
	cache.bytes -= oldest.bytes
}

func (cache *columnarDecompressedBlockCache) loadedBlockCountLocked() int {
	count := 0
	for _, column := range cache.columns {
		for index := range column.blocks {
			if column.blocks[index].Load() != nil {
				count++
			}
		}
	}
	return count
}

func (cache *columnarDecompressedBlockCache) loadedBlocks() []columnarDecompressedBlockKey {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	keys := make([]columnarDecompressedBlockKey, 0, cache.loadedBlockCountLocked())
	for field, column := range cache.columns {
		for index := range column.blocks {
			if column.blocks[index].Load() != nil {
				keys = append(keys, columnarDecompressedBlockKey{field: field, block: index})
			}
		}
	}
	return keys
}

func (batch ColumnarBatch) hasDecompressedBlockColumns() bool {
	return len(batch.Dictionaries) > 0 || len(batch.PackedColumns) > 0 || len(batch.BoolColumns) > 0 || len(batch.NumericColumns) > 0
}

func (batch ColumnarBatch) decodeDecompressedBlock(field string, block int) columnarDecompressedBlock {
	cache := batch.decompressedBlockCache
	if cache == nil || cache.blockRows <= 0 {
		return columnarDecompressedBlock{}
	}
	start := block * cache.blockRows
	if start < 0 || start >= batch.Rows {
		return columnarDecompressedBlock{}
	}
	end := batch.Rows
	if remaining := batch.Rows - start; remaining > cache.blockRows {
		end = start + cache.blockRows
	}
	values := make([]interface{}, end-start)
	validity := make([]byte, columnarPackedBitmapBytes(len(values)))
	for row := start; row < end; row++ {
		value, valid := batch.valueWithoutDecompressedCache(field, row)
		index := row - start
		values[index] = value
		if valid {
			validity[index>>3] |= byte(1 << uint(index&7))
		}
	}
	return columnarDecompressedBlock{
		values:   values,
		validity: validity,
		bytes:    64 + len(values)*16 + len(validity),
	}
}

func (block columnarDecompressedBlock) validAt(offset int) bool {
	if offset < 0 || offset >= len(block.values) || offset>>3 >= len(block.validity) {
		return false
	}
	return block.validity[offset>>3]&(byte(1)<<uint(offset&7)) != 0
}
