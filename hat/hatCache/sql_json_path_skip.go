package hatCache

import (
	"fmt"

	"hatrie_cache/hat/hatSql"
)

const (
	DefaultSQLJSONPathSkipRowsPerSegment = 256
	DefaultSQLJSONPathSkipBitsPerSegment = 512
	DefaultSQLJSONPathSkipMaxPaths       = 64
	defaultSQLJSONPathSkipMaxBits        = 1 << 20
	defaultSQLJSONPathSkipProbeCount     = 4
)

// SQLJSONPathSkipIndexSpec configures bounded per-segment equality metadata for
// one CACHE JSON source. It stores no value postings; Bloom false positives are
// rechecked by the SQL executor against the original predicate.
type SQLJSONPathSkipIndexSpec struct {
	CacheKey       string
	Paths          []string
	RowsPerSegment int
	BitsPerSegment int
}

type sqlJSONPathSkipIndex struct {
	sqlJSONIndexState
	path            string
	rows            []SQLRow
	rowsPerSegment  int
	bitsPerSegment  int
	wordsPerSegment int
	bits            []uint64
}

func normalizeSQLJSONPathSkipIndexSpec(spec SQLJSONPathSkipIndexSpec) ([]string, int, int, error) {
	if spec.CacheKey == "" || len(spec.Paths) == 0 {
		return nil, 0, 0, fmt.Errorf("SQL JSON path skip index requires a cache key and path")
	}
	if len(spec.Paths) > DefaultSQLJSONPathSkipMaxPaths {
		return nil, 0, 0, fmt.Errorf("SQL JSON path skip index supports at most %d paths", DefaultSQLJSONPathSkipMaxPaths)
	}
	rowsPerSegment := spec.RowsPerSegment
	if rowsPerSegment <= 0 {
		rowsPerSegment = DefaultSQLJSONPathSkipRowsPerSegment
	}
	if rowsPerSegment > 1<<20 {
		return nil, 0, 0, fmt.Errorf("SQL JSON path skip rows per segment exceeds the configured bound")
	}
	bitsPerSegment := spec.BitsPerSegment
	if bitsPerSegment <= 0 {
		bitsPerSegment = DefaultSQLJSONPathSkipBitsPerSegment
	}
	if bitsPerSegment < 64 {
		bitsPerSegment = 64
	}
	if bitsPerSegment > defaultSQLJSONPathSkipMaxBits-63 {
		return nil, 0, 0, fmt.Errorf("SQL JSON path skip bits per segment exceeds the configured bound")
	}
	bitsPerSegment = (bitsPerSegment + 63) &^ 63
	paths := make([]string, 0, len(spec.Paths))
	seen := make(map[string]struct{}, len(spec.Paths))
	for _, path := range spec.Paths {
		normalized, err := hatSql.NormalizeJSONPath(path)
		if err != nil {
			return nil, 0, 0, err
		}
		if _, exists := seen[normalized]; exists {
			return nil, 0, 0, fmt.Errorf("SQL JSON path skip index path %q is repeated", normalized)
		}
		seen[normalized] = struct{}{}
		paths = append(paths, normalized)
	}
	return paths, rowsPerSegment, bitsPerSegment, nil
}

// CreateSQLJSONPathSkipIndex configures bounded equality skip metadata for the
// supplied JSON paths. Existing full path indexes take precedence when both
// are configured. The feature is opt-in and does not change source semantics.
func (ht *HatTrie) CreateSQLJSONPathSkipIndex(spec SQLJSONPathSkipIndexSpec) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	paths, rowsPerSegment, bitsPerSegment, err := normalizeSQLJSONPathSkipIndexSpec(spec)
	if err != nil {
		return err
	}
	ht.registerSQLJSONIndexSource(spec.CacheKey)
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONPathSkipIndexes == nil {
		ht.sqlJSONPathSkipIndexes = make(map[string]map[string]*sqlJSONPathSkipIndex)
	}
	if ht.sqlJSONPathSkipIndexes[spec.CacheKey] == nil {
		ht.sqlJSONPathSkipIndexes[spec.CacheKey] = make(map[string]*sqlJSONPathSkipIndex)
	}
	configured := len(ht.sqlJSONPathSkipIndexes[spec.CacheKey])
	for _, path := range paths {
		if _, exists := ht.sqlJSONPathSkipIndexes[spec.CacheKey][path]; !exists {
			configured++
		}
	}
	if configured > DefaultSQLJSONPathSkipMaxPaths {
		return fmt.Errorf("SQL JSON path skip index supports at most %d paths per source", DefaultSQLJSONPathSkipMaxPaths)
	}
	for _, path := range paths {
		ht.sqlJSONPathSkipIndexes[spec.CacheKey][path] = &sqlJSONPathSkipIndex{
			path:            path,
			rowsPerSegment:  rowsPerSegment,
			bitsPerSegment:  bitsPerSegment,
			wordsPerSegment: bitsPerSegment / 64,
		}
	}
	return nil
}

func sqlJSONPathSkipHash(value string) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for index := 0; index < len(value); index++ {
		hash ^= uint64(value[index])
		hash *= prime
	}
	return hash
}

func sqlJSONPathSkipProbeHash(hash, probe uint64) uint64 {
	mixed := hash + (probe+1)*0x9e3779b97f4a7c15
	mixed ^= mixed >> 30
	mixed *= 0xbf58476d1ce4e5b9
	mixed ^= mixed >> 27
	mixed *= 0x94d049bb133111eb
	return mixed ^ mixed>>31
}

func sqlJSONPathSkipBitIndex(hash, probe uint64, bits int) int {
	return int(sqlJSONPathSkipProbeHash(hash, probe) % uint64(bits))
}

func sqlJSONPathSkipAdd(bits []uint64, offset, bitsPerSegment int, value string) {
	hash := sqlJSONPathSkipHash(value)
	for probe := uint64(0); probe < defaultSQLJSONPathSkipProbeCount; probe++ {
		bit := sqlJSONPathSkipBitIndex(hash, probe, bitsPerSegment)
		bits[offset+bit/64] |= uint64(1) << uint(bit%64)
	}
}

func sqlJSONPathSkipMayContain(bits []uint64, offset, bitsPerSegment int, value string) bool {
	hash := sqlJSONPathSkipHash(value)
	for probe := uint64(0); probe < defaultSQLJSONPathSkipProbeCount; probe++ {
		bit := sqlJSONPathSkipBitIndex(hash, probe, bitsPerSegment)
		if bits[offset+bit/64]&(uint64(1)<<uint(bit%64)) == 0 {
			return false
		}
	}
	return true
}

func refreshSQLJSONPathSkipIndexSource(index *sqlJSONPathSkipIndex, source sqlJSONSource, rows []SQLRow) error {
	if source.current(index.sqlJSONIndexState) {
		return nil
	}
	if index.rowsPerSegment <= 0 || index.wordsPerSegment <= 0 {
		return fmt.Errorf("SQL JSON path skip index has invalid segment configuration")
	}
	segments := (len(rows) + index.rowsPerSegment - 1) / index.rowsPerSegment
	maxInt := int(^uint(0) >> 1)
	if segments > maxInt/index.wordsPerSegment {
		return fmt.Errorf("SQL JSON path skip index metadata is too large")
	}
	metadata := make([]uint64, segments*index.wordsPerSegment)
	for ordinal, row := range rows {
		value, exists, err := sqlJSONIndexRowValue(row, index.path)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		valueKey, ok := sqlIndexValueKey(value)
		if !ok {
			continue
		}
		segment := ordinal / index.rowsPerSegment
		offset := segment * index.wordsPerSegment
		sqlJSONPathSkipAdd(metadata, offset, index.bitsPerSegment, valueKey)
	}
	index.sqlJSONIndexState = sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}
	index.rows, index.bits = rows, metadata
	return nil
}

func sqlJSONPathSkipRows(index *sqlJSONPathSkipIndex, value interface{}) ([]SQLRow, bool) {
	valueKey, ok := sqlIndexValueKey(value)
	if !ok {
		return nil, false
	}
	rows := make([]SQLRow, 0)
	for segment, start := 0, 0; start < len(index.rows); segment, start = segment+1, start+index.rowsPerSegment {
		offset := segment * index.wordsPerSegment
		if !sqlJSONPathSkipMayContain(index.bits, offset, index.bitsPerSegment, valueKey) {
			continue
		}
		end := start + index.rowsPerSegment
		if end > len(index.rows) {
			end = len(index.rows)
		}
		rows = append(rows, index.rows[start:end]...)
	}
	return hatSql.CloneRows(rows), true
}
