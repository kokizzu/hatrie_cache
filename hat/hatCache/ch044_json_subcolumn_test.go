package hatCache

import (
	"errors"
	"reflect"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestCH044HatTrieJSONSubcolumnMaterializerIsOptInAndInvalidates(t *testing.T) {
	trie := newTestTrie(t)
	path := []hatSql.ColumnarJSONSubcolumnRequest{{Field: "doc", Path: "$.user.id"}}
	fields := []string{"doc"}
	source := `[{"doc":{"user":{"id":7,"name":"ann"}}},{"doc":{"user":{"name":"bob"}}},{"doc":{"user":{"id":null}}}]`
	if err := trie.UpsertStringChecked("users", source); err != nil {
		t.Fatalf("UpsertStringChecked() error = %v", err)
	}

	if _, _, available, err := trie.ResolveSQLColumnarJSONSubcolumns("CACHE", "users", fields, path); err != nil || available {
		t.Fatalf("default resolver = available %v, err %v; want disabled", available, err)
	}
	if err := trie.ConfigureSQLJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{
		MinObservations: 2,
		MaxEntries:      4,
		MaxRows:         32,
		MaxBytes:        4096,
	}); err != nil {
		t.Fatalf("ConfigureSQLJSONSubcolumnAutoMaterializer() error = %v", err)
	}

	if _, _, available, err := trie.ResolveSQLColumnarJSONSubcolumns("CACHE", "users", fields, path); err != nil || available {
		t.Fatalf("cold resolver = available %v, err %v; want unavailable", available, err)
	}
	batch, _, available, err := trie.ResolveSQLColumnarJSONSubcolumns("CACHE", "users", fields, path)
	if err != nil || !available {
		t.Fatalf("promoted resolver = available %v, err %v; want available", available, err)
	}
	column, ok := batch.JSONSubcolumns[hatSql.ColumnarJSONSubcolumnKey{Field: "doc", Path: "$.user.id"}]
	if !ok {
		t.Fatal("promoted resolver did not return requested JSON subcolumn")
	}
	if value, present := column.Value(0); !present || value != float64(7) {
		t.Fatalf("row 0 value = %#v/%v, want 7/true", value, present)
	}
	if value, present := column.Value(1); present {
		t.Fatalf("row 1 value = %#v/%v, want missing/false", value, present)
	}
	if value, present := column.Value(2); !present || value != nil {
		t.Fatalf("row 2 value = %#v/%v, want null/true", value, present)
	}
	if got := trie.SQLJSONSubcolumnAutoMaterializerStats(); got.Promotions != 1 || got.Entries != 1 {
		t.Fatalf("materializer stats = %+v, want one promotion and entry", got)
	}
	if len(trie.sqlJSONIndexSnapshots) != 0 {
		t.Fatalf("JSON subcolumn materialization retained %d full-row snapshots, want none", len(trie.sqlJSONIndexSnapshots))
	}

	query := "FROM CACHE('users') AS user WHERE JSON_VALUE(user.doc, '$.user.id') >= 7 SELECT JSON_VALUE(user.doc, '$.user.id') AS id"
	columnar, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("columnar ExecuteSQLQuery() error = %v", err)
	}
	materialized, err := ExecuteSQLQuery(query, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatalf("row ExecuteSQLQuery() error = %v", err)
	}
	if !reflect.DeepEqual(columnar, materialized) {
		t.Fatalf("columnar result = %#v, row result = %#v", columnar, materialized)
	}
	queryWithField := "FROM CACHE('users') AS user WHERE JSON_VALUE(user.doc, '$.user.id') >= 7 SELECT user.doc, JSON_VALUE(user.doc, '$.user.id') AS id"
	columnar, err = ExecuteSQLQuery(queryWithField, trie)
	if err != nil {
		t.Fatalf("columnar mixed ExecuteSQLQuery() error = %v", err)
	}
	materialized, err = ExecuteSQLQuery(queryWithField, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatalf("row mixed ExecuteSQLQuery() error = %v", err)
	}
	if !reflect.DeepEqual(columnar, materialized) {
		t.Fatalf("mixed columnar result = %#v, row result = %#v", columnar, materialized)
	}

	trie.DisableSQLJSONSubcolumnAutoMaterializer()
	if got := trie.SQLJSONSubcolumnAutoMaterializerStats(); got.Entries != 0 || got.RetainedBytes != 0 {
		t.Fatalf("disabled materializer stats = %+v, want empty", got)
	}
	if _, _, available, err := trie.ResolveSQLColumnarJSONSubcolumns("CACHE", "users", fields, path); err != nil || available {
		t.Fatalf("disabled resolver = available %v, err %v; want unavailable", available, err)
	}
	if err := trie.ConfigureSQLJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{
		MinObservations: 2,
		MaxEntries:      4,
		MaxRows:         32,
		MaxBytes:        4096,
	}); err != nil {
		t.Fatalf("re-enable ConfigureSQLSubcolumnAutoMaterializer() error = %v", err)
	}

	updated := `[{"doc":{"user":{"id":8,"name":"ann"}}},{"doc":{"user":{"id":9}}},{"doc":{"user":{"id":10}}}]`
	if err := trie.UpsertStringChecked("users", updated); err != nil {
		t.Fatalf("updated UpsertStringChecked() error = %v", err)
	}
	if _, _, available, err := trie.ResolveSQLColumnarJSONSubcolumns("CACHE", "users", fields, path); err != nil || available {
		t.Fatalf("post-write cold resolver = available %v, err %v; want unavailable", available, err)
	}
	batch, _, available, err = trie.ResolveSQLColumnarJSONSubcolumns("CACHE", "users", fields, path)
	if err != nil || !available {
		t.Fatalf("post-write promoted resolver = available %v, err %v; want available", available, err)
	}
	column = batch.JSONSubcolumns[hatSql.ColumnarJSONSubcolumnKey{Field: "doc", Path: "$.user.id"}]
	if value, present := column.Value(0); !present || value != float64(8) {
		t.Fatalf("post-write row 0 value = %#v/%v, want 8/true", value, present)
	}
}

func TestCH044HatTrieJSONSubcolumnMaterializerConfigValidation(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureSQLJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{MaxEntries: -1}); !errors.Is(err, hatSql.ErrJSONSubcolumnAutoMaterializerConfig) {
		t.Fatalf("invalid config error = %v, want %v", err, hatSql.ErrJSONSubcolumnAutoMaterializerConfig)
	}
	if err := (*HatTrie)(nil).ConfigureSQLJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{}); !errors.Is(err, ErrNilHatTrie) {
		t.Fatalf("nil trie config error = %v, want %v", err, ErrNilHatTrie)
	}
}

func TestCH044JSONSubcolumnAccessObservationDefersRowDecode(t *testing.T) {
	materializer, err := hatSql.NewJSONSubcolumnAutoMaterializer(hatSql.JSONSubcolumnAutoMaterializerOptions{MinObservations: 2})
	if err != nil {
		t.Fatal(err)
	}
	source := hatSql.JSONSubcolumnAutoSource{SourceName: "CACHE", SourceKey: "users", Generation: 1}
	paths := []hatSql.ColumnarJSONSubcolumnRequest{{Field: "doc", Path: "$.id"}}
	if ready, err := materializer.ObserveRequests(source, paths); err != nil || ready {
		t.Fatalf("first ObserveRequests() = ready %v, err %v; want not ready", ready, err)
	}
	if ready, err := materializer.ObserveRequests(source, paths); err != nil || !ready {
		t.Fatalf("second ObserveRequests() = ready %v, err %v; want ready", ready, err)
	}
}
