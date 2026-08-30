package hatCache

import (
	"sort"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"hatrie_cache/hat/hatSql"
)

var sqlIndexFreshnessBenchmarkResult bool

func sqlIndexSameImmutableStorage(left, right string) bool {
	return len(left) == len(right) && unsafe.StringData(left) == unsafe.StringData(right)
}

// sqlOrderedRowsOnlyResolver exposes the pre-streaming ordered-index contract.
// It keeps the benchmark's materialized control on the same underlying index.
type sqlOrderedRowsOnlyResolver struct{ trie *HatTrie }

func (resolver sqlOrderedRowsOnlyResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func (resolver sqlOrderedRowsOnlyResolver) ResolveSQLOrderedSource(name, key, field string, desc, nullsFirst, nullsLast bool) ([]SQLRow, bool, error) {
	return resolver.trie.ResolveSQLOrderedSource(name, key, field, desc, nullsFirst, nullsLast)
}

// sqlCopiedCoveringResolver retains the pre-immutable-source covering path.
// It uses the same configured index as the optimized resolver for comparison.
type sqlCopiedCoveringResolver struct{ trie *HatTrie }

func (resolver sqlCopiedCoveringResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

// sqlCopiedSecondaryResolver retains the pre-immutable-source secondary index
// paths. It is benchmark-only and keeps the same configured indexes as the
// optimized resolver.
type sqlCopiedSecondaryResolver struct{ trie *HatTrie }

func (resolver sqlCopiedSecondaryResolver) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	data, err := resolver.trie.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	resolver.trie.sqlIndexMu.Lock()
	defer resolver.trie.sqlIndexMu.Unlock()
	index := resolver.trie.sqlJSONBitmapIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONBitmapIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	valueKey, ok := sqlIndexValueKey(value)
	if !ok {
		return []SQLRow{}, true, nil
	}
	ordinals := index.postings[valueKey].Values()
	rows := make([]SQLRow, 0, len(ordinals))
	for _, ordinal := range ordinals {
		if int(ordinal) < len(index.rows) {
			rows = append(rows, index.rows[ordinal])
		}
	}
	return hatSql.CloneRows(rows), true, nil
}

func (resolver sqlCopiedSecondaryResolver) ResolveSQLTextSource(name, key, field, query string) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	data, err := resolver.trie.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	resolver.trie.sqlIndexMu.Lock()
	defer resolver.trie.sqlIndexMu.Unlock()
	index := resolver.trie.sqlJSONTextIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONTextIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	tokens := hatSql.TextTokens(query)
	if len(tokens) == 0 {
		return []SQLRow{}, true, nil
	}
	postings := make([][]int, len(tokens))
	for tokenIndex, token := range tokens {
		posting := index.tokens[token]
		if len(posting) == 0 {
			return []SQLRow{}, true, nil
		}
		postings[tokenIndex] = posting
	}
	sort.Slice(postings, func(left, right int) bool { return len(postings[left]) < len(postings[right]) })
	matched := append([]int(nil), postings[0]...)
	for _, posting := range postings[1:] {
		matched = intersectSQLTextPostings(matched, posting)
		if len(matched) == 0 {
			return []SQLRow{}, true, nil
		}
	}
	rows := make([]SQLRow, len(matched))
	for rowIndex, sourceIndex := range matched {
		rows[rowIndex] = index.rows[sourceIndex]
	}
	return hatSql.CloneRows(rows), true, nil
}

func (resolver sqlCopiedSecondaryResolver) ResolveSQLCompositeIndexedSource(name, key string, fields []string, values []interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" || len(fields) != len(values) || len(fields) < 2 {
		return nil, false, nil
	}
	data, err := resolver.trie.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	provided := make(map[string]interface{}, len(fields))
	for index, field := range fields {
		provided[field] = values[index]
	}
	resolver.trie.sqlIndexMu.Lock()
	defer resolver.trie.sqlIndexMu.Unlock()
	var selected *sqlJSONCompositeIndex
	for _, candidate := range resolver.trie.sqlJSONCompositeIndexes[key] {
		if len(candidate.fields) <= 1 || selected != nil && len(candidate.fields) <= len(selected.fields) {
			continue
		}
		available := true
		for _, field := range candidate.fields {
			if _, ok := provided[field]; !ok {
				available = false
				break
			}
		}
		if available {
			selected = candidate
		}
	}
	if selected == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONCompositeIndex(selected, key, data); err != nil {
		return nil, false, err
	}
	lookup := make([]interface{}, len(selected.fields))
	for index, field := range selected.fields {
		lookup[index] = provided[field]
	}
	valueKey, ok := sqlJSONCompositeIndexValueKey(lookup)
	if !ok {
		return []SQLRow{}, true, nil
	}
	return hatSql.CloneRows(selected.rows[valueKey]), true, nil
}

func (resolver sqlCopiedCoveringResolver) ResolveSQLCoveringSource(name, key, field string, value interface{}, fields []string) ([]SQLRow, bool, error) {
	if name != "CACHE" || len(fields) == 0 {
		return nil, false, nil
	}
	data, err := resolver.trie.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	resolver.trie.sqlIndexMu.Lock()
	defer resolver.trie.sqlIndexMu.Unlock()
	index := resolver.trie.sqlJSONCoveringIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	for _, column := range fields {
		if _, ok := index.columns[column]; !ok {
			return nil, false, nil
		}
	}
	if err := refreshSQLJSONCoveringIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	valueKey, ok := sqlIndexValueKey(value)
	if !ok {
		return []SQLRow{}, true, nil
	}
	return hatSql.CloneRows(index.rows[valueKey]), true, nil
}

func BenchmarkSQLTypedIndexBaseline(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	const rows = 100_000
	var data strings.Builder
	data.Grow(rows * 32)
	data.WriteByte('[')
	for index := 0; index < rows; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(index))
		data.WriteString(`,"state":"ready"}`)
	}
	data.WriteByte(']')
	source := data.String()
	trie.UpsertString("events", source)
	if err := trie.CreateSQLJSONFieldIndex("events", "id"); err != nil {
		b.Fatal(err)
	}
	covering := CreateHatTrie()
	b.Cleanup(covering.Destroy)
	covering.UpsertString("events", source)
	if err := covering.CreateSQLJSONCoveringIndex("events", "id", "id"); err != nil {
		b.Fatal(err)
	}
	equalityQuery := "FROM CACHE('events') AS event WHERE event.id = 99999 SELECT event.id"
	rangeQuery := "FROM CACHE('events') AS event WHERE event.id >= 99900 AND event.id < 99910 SELECT event.id"
	orderQuery := "FROM CACHE('events') AS event ORDER BY event.id DESC LIMIT 10 SELECT event.id"
	if result, err := ExecuteSQLQuery(equalityQuery, trie); err != nil || len(result.Rows) != 1 {
		b.Fatalf("indexed equality warmup = %#v, %v", result, err)
	}
	if result, err := ExecuteSQLQuery(rangeQuery, trie); err != nil || len(result.Rows) != 10 {
		b.Fatalf("indexed range warmup = %#v, %v", result, err)
	}
	if result, err := ExecuteSQLQuery(orderQuery, trie); err != nil || len(result.Rows) != 10 {
		b.Fatalf("indexed order warmup = %#v, %v", result, err)
	}
	if result, err := ExecuteSQLQuery(equalityQuery, covering); err != nil || len(result.Rows) != 1 {
		b.Fatalf("covering equality warmup = %#v, %v", result, err)
	}
	fullScan := sqlRowsOnlyResolver{trie: trie}
	materializedIndex := sqlOrderedRowsOnlyResolver{trie: trie}
	copiedCovering := sqlCopiedCoveringResolver{trie: covering}
	for _, benchmark := range []struct {
		name     string
		query    string
		resolver SQLSourceResolver
	}{
		{name: "equality/indexed", query: equalityQuery, resolver: trie},
		{name: "equality/full_scan", query: equalityQuery, resolver: fullScan},
		{name: "covering/equality_immutable_source", query: equalityQuery, resolver: covering},
		{name: "covering/equality_copied_source", query: equalityQuery, resolver: copiedCovering},
		{name: "range/indexed", query: rangeQuery, resolver: trie},
		{name: "range/full_scan", query: rangeQuery, resolver: fullScan},
		{name: "order_limit/streamed_index", query: orderQuery, resolver: trie},
		{name: "order_limit/materialized_index", query: orderQuery, resolver: materializedIndex},
		{name: "order_limit/full_scan", query: orderQuery, resolver: fullScan},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := ExecuteSQLQuery(benchmark.query, benchmark.resolver); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("equality/direct_indexed_resolver", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			resolved, available, err := trie.ResolveSQLIndexedSource("CACHE", "events", "id", float64(rows-1))
			if err != nil || !available || len(resolved) != 1 {
				b.Fatalf("ResolveSQLIndexedSource() = %#v, %v, %v", resolved, available, err)
			}
		}
	})
}

func BenchmarkSQLSecondaryIndexSource(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	const rows = 100_000
	var data strings.Builder
	data.Grow(rows * 80)
	data.WriteByte('[')
	for index := 0; index < rows; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		state, body, team, enabled := "idle", "ordinary event", "core", "false"
		if index == rows-1 {
			state, body, team, enabled = "ready", "fast cache lookup", "edge", "true"
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(index))
		data.WriteString(`,"state":"`)
		data.WriteString(state)
		data.WriteString(`","body":"`)
		data.WriteString(body)
		data.WriteString(`","team":"`)
		data.WriteString(team)
		data.WriteString(`","enabled":`)
		data.WriteString(enabled)
		data.WriteByte('}')
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())
	if err := trie.CreateSQLJSONBitmapIndex("events", "state"); err != nil {
		b.Fatal(err)
	}
	if err := trie.CreateSQLJSONTextIndex("events", "body"); err != nil {
		b.Fatal(err)
	}
	if err := trie.CreateSQLJSONCompositeIndex("events", "team", "enabled"); err != nil {
		b.Fatal(err)
	}
	if resolved, available, err := trie.ResolveSQLIndexedSource("CACHE", "events", "state", "ready"); err != nil || !available || len(resolved) != 1 {
		b.Fatalf("bitmap warmup = %d/%t/%v", len(resolved), available, err)
	}
	if resolved, available, err := trie.ResolveSQLTextSource("CACHE", "events", "body", "fast"); err != nil || !available || len(resolved) != 1 {
		b.Fatalf("text warmup = %d/%t/%v", len(resolved), available, err)
	}
	if resolved, available, err := trie.ResolveSQLCompositeIndexedSource("CACHE", "events", []string{"team", "enabled"}, []interface{}{"edge", true}); err != nil || !available || len(resolved) != 1 {
		b.Fatalf("composite warmup = %d/%t/%v", len(resolved), available, err)
	}
	copied := sqlCopiedSecondaryResolver{trie: trie}
	for _, benchmark := range []struct {
		name string
		run  func() error
	}{
		{"bitmap/immutable_source", func() error {
			_, _, err := trie.ResolveSQLIndexedSource("CACHE", "events", "state", "ready")
			return err
		}},
		{"bitmap/copied_source", func() error {
			_, _, err := copied.ResolveSQLIndexedSource("CACHE", "events", "state", "ready")
			return err
		}},
		{"text/immutable_source", func() error { _, _, err := trie.ResolveSQLTextSource("CACHE", "events", "body", "fast"); return err }},
		{"text/copied_source", func() error { _, _, err := copied.ResolveSQLTextSource("CACHE", "events", "body", "fast"); return err }},
		{"composite/immutable_source", func() error {
			_, _, err := trie.ResolveSQLCompositeIndexedSource("CACHE", "events", []string{"team", "enabled"}, []interface{}{"edge", true})
			return err
		}},
		{"composite/copied_source", func() error {
			_, _, err := copied.ResolveSQLCompositeIndexedSource("CACHE", "events", []string{"team", "enabled"}, []interface{}{"edge", true})
			return err
		}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := benchmark.run(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSQLIndexFreshnessIdentity(b *testing.B) {
	source := strings.Repeat(`{"id":1,"state":"ready"}`, 250_000)
	clone := strings.Clone(source)
	for _, benchmark := range []struct {
		name string
		left string
		run  func(string, string) bool
	}{
		{"string_equality/same_storage", source, func(left, right string) bool { return left == right }},
		{"identity/same_storage", source, sqlIndexSameImmutableStorage},
		{"string_equality/equal_content_copy", clone, func(left, right string) bool { return left == right }},
		{"identity/equal_content_copy", clone, sqlIndexSameImmutableStorage},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				sqlIndexFreshnessBenchmarkResult = benchmark.run(benchmark.left, source)
			}
		})
	}
}
