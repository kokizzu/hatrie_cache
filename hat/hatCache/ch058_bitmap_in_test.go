package hatCache

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestSQLBitmapIndexBatchINResolverPreservesLiteralOrderAndDuplicates(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":1,"state":"ready"},
  {"id":2,"state":"queued"},
  {"id":3,"state":"done"},
  {"id":4,"state":"ready"},
  {"id":5,"state":"done"}
]`)
	if err := trie.CreateSQLJSONBitmapIndex("jobs", "state"); err != nil {
		t.Fatalf("CreateSQLJSONBitmapIndex() error = %v", err)
	}

	rows, available, err := trie.ResolveSQLIndexedValues("CACHE", "jobs", "state", []interface{}{"ready", "done", "ready", nil})
	if err != nil || !available {
		t.Fatalf("ResolveSQLIndexedValues() = %#v, %v, %v", rows, available, err)
	}
	want := []SQLRow{
		{"id": float64(1), "state": "ready"},
		{"id": float64(4), "state": "ready"},
		{"id": float64(3), "state": "done"},
		{"id": float64(5), "state": "done"},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("batch rows = %#v, want %#v", rows, want)
	}

	result, err := ExecuteSQLQuery("FROM CACHE('jobs') AS job WHERE job.state IN ('ready', 'done', 'ready') SELECT job.id", trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	wantResult := []SQLRow{{"id": float64(1)}, {"id": float64(4)}, {"id": float64(3)}, {"id": float64(5)}}
	if !reflect.DeepEqual(result.Rows, wantResult) {
		t.Fatalf("query rows = %#v, want %#v", result.Rows, wantResult)
	}
}

func TestSQLBitmapIndexBatchINResolverDeclinesWithoutBitmapIndex(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"ready"}]`)
	rows, available, err := trie.ResolveSQLIndexedValues("CACHE", "jobs", "state", []interface{}{"ready"})
	if err != nil {
		t.Fatalf("ResolveSQLIndexedValues() error = %v", err)
	}
	if available || rows != nil {
		t.Fatalf("ResolveSQLIndexedValues() = %#v, %v; want unavailable", rows, available)
	}
}

func TestSQLBitmapIndexBatchINFallsBackToGenericIndex(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[
  {"id":1,"state":"ready"},
  {"id":2,"state":"queued"},
  {"id":3,"state":"done"}
]`)
	if err := trie.CreateSQLJSONFieldIndex("jobs", "state"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	result, err := ExecuteSQLQuery("FROM CACHE('jobs') AS job WHERE job.state IN ('ready', 'done') SELECT job.id", trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := []SQLRow{{"id": float64(1)}, {"id": float64(3)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("query rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLBitmapIndexBatchINResolverReadsDensePostingContainers(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	var data strings.Builder
	data.Grow(300_000)
	data.WriteByte('[')
	for row := 0; row < 9_000; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		fmt.Fprintf(&data, `{"id":%d,"state":"ready"}`, row)
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())
	if err := trie.CreateSQLJSONBitmapIndex("jobs", "state"); err != nil {
		t.Fatalf("CreateSQLJSONBitmapIndex() error = %v", err)
	}
	rows, available, err := trie.ResolveSQLIndexedValues("CACHE", "jobs", "state", []interface{}{"ready"})
	if err != nil || !available || len(rows) != 9_000 {
		t.Fatalf("ResolveSQLIndexedValues() = %d rows, %v, %v; want 9000, true, nil", len(rows), available, err)
	}
	if rows[0]["id"] != float64(0) || rows[len(rows)-1]["id"] != float64(8_999) {
		t.Fatalf("dense rows endpoints = %#v/%#v", rows[0], rows[len(rows)-1])
	}
}
