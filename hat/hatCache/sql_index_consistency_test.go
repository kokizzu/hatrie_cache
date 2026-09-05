package hatCache

import (
	"errors"
	"testing"
)

func TestSQLJSONIndexConsistencyBoundaries(t *testing.T) {
	var nilTrie *HatTrie
	if _, available, err := nilTrie.CheckSQLJSONIndexConsistency("people"); !errors.Is(err, ErrNilHatTrie) || available {
		t.Fatalf("nil CheckSQLJSONIndexConsistency() = %v, %v, want ErrNilHatTrie/false", available, err)
	}

	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"team":"blue"}]`)
	if report, available, err := trie.CheckSQLJSONIndexConsistency("people"); err != nil || available || report.Consistent {
		t.Fatalf("unconfigured consistency report = %#v, %v, %v", report, available, err)
	}

	if err := trie.CreateSQLJSONFieldIndex("people", "team"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	if err := trie.SetSQLJSONIndexAdmissionBudget(SQLJSONIndexAdmissionBudget{MaxSourceBytes: 1}); err != nil {
		t.Fatalf("SetSQLJSONIndexAdmissionBudget() error = %v", err)
	}
	if _, available, err := trie.CheckSQLJSONIndexConsistency("people"); !available || !errors.Is(err, errSQLJSONIndexAdmissionDenied) {
		t.Fatalf("admission-limited consistency check = %v, %v, want available/denied", available, err)
	}
}

func TestSQLJSONIndexConsistencyCoversAllConfiguredIndexKinds(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("events", `[
{"id":1,"team":"blue","name":"Alice","status":"active","city":"Singapore","body":"fast cache","tenant_id":10,"created_at":100},
{"id":2,"team":"blue","name":"Bob","status":"active","city":"Jakarta","body":"durable cache","tenant_id":10,"created_at":200},
{"id":3,"team":"red","name":"Cara","status":"paused","city":"Singapore","body":"query cache","tenant_id":20,"created_at":300}
]`)
	create := []func() error{
		func() error { return trie.CreateSQLJSONFieldIndex("events", "team") },
		func() error {
			return trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{CacheKey: "events", Fields: []string{"id"}, Type: SQLIndexInt64})
		},
		func() error { return trie.CreateSQLJSONLowerIndex("events", "name") },
		func() error { return trie.CreateSQLJSONBitmapIndex("events", "team") },
		func() error {
			return trie.CreateSQLJSONPathSkipIndex(SQLJSONPathSkipIndexSpec{CacheKey: "events", Paths: []string{"$.city"}})
		},
		func() error { return trie.CreateSQLJSONCoveringIndex("events", "team", "id", "name") },
		func() error { return trie.CreateSQLJSONTextIndex("events", "body") },
		func() error { return trie.CreateSQLJSONCompositeIndex("events", "team", "status") },
		func() error {
			return trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{CacheKey: "events", Fields: []string{"tenant_id", "created_at"}, Type: SQLIndexInt64})
		},
		func() error { return trie.CreateSQLJSONPartialIndex("events", "name", "status", "active") },
	}
	for _, configure := range create {
		if err := configure(); err != nil {
			t.Fatalf("configure index: %v", err)
		}
	}

	source, err := trie.sqlJSONSource("events")
	if err != nil {
		t.Fatalf("sqlJSONSource() error = %v", err)
	}
	rows, err := sqlJSONRowsString("events", source.raw)
	if err != nil {
		t.Fatalf("sqlJSONRowsString() error = %v", err)
	}
	trie.sqlIndexMu.Lock()
	refreshSQLJSONTypedInt64IndexSource(trie.sqlJSONTypedInt64Indexes["events"]["id"], "id", source, rows)
	refreshSQLJSONFieldIndexSourceRows(trie.sqlJSONIndexes["events"]["team"], "team", source, rows)
	refreshSQLJSONLowerIndexSource(trie.sqlJSONLowerIndexes["events"]["name"], "name", source, rows)
	if err := refreshSQLJSONBitmapIndexSourceRows(trie.sqlJSONBitmapIndexes["events"]["team"], "team", source, rows); err != nil {
		t.Fatalf("refresh bitmap index: %v", err)
	}
	if err := refreshSQLJSONPathSkipIndexSource(trie.sqlJSONPathSkipIndexes["events"]["$.city"], source, rows); err != nil {
		t.Fatalf("refresh path skip index: %v", err)
	}
	if err := refreshSQLJSONCoveringIndexSource(trie.sqlJSONCoveringIndexes["events"]["team"], "events", "team", source); err != nil {
		t.Fatalf("refresh covering index: %v", err)
	}
	if err := refreshSQLJSONTextIndexSourceRows(trie.sqlJSONTextIndexes["events"]["body"], "body", source, rows); err != nil {
		t.Fatalf("refresh text index: %v", err)
	}
	if err := refreshSQLJSONCompositeIndexSourceRows(trie.sqlJSONCompositeIndexes["events"][sqlJSONCompositeIndexIdentifier([]string{"team", "status"})], source, rows); err != nil {
		t.Fatalf("refresh composite index: %v", err)
	}
	if err := refreshSQLJSONTypedInt64CompositeIndexSource(trie.sqlJSONTypedInt64CompositeIndexes["events"][sqlJSONCompositeIndexIdentifier([]string{"tenant_id", "created_at"})], source, rows); err != nil {
		t.Fatalf("refresh typed composite index: %v", err)
	}
	if err := refreshSQLJSONPartialIndexSource(trie.sqlJSONPartialIndexes["events"]["name\x00status\x00s:active"], source, rows); err != nil {
		t.Fatalf("refresh partial index: %v", err)
	}
	trie.sqlIndexMu.Unlock()

	report, available, err := trie.CheckSQLJSONIndexConsistency("events")
	if err != nil || !available || !report.Consistent || report.SourceRows != 3 || len(report.Indexes) != len(create) {
		t.Fatalf("all-index consistency report = %#v, %v, %v", report, available, err)
	}
	for _, index := range report.Indexes {
		if !index.Ready || !index.Current || !index.Consistent {
			t.Fatalf("index consistency = %#v", index)
		}
	}
}

func TestSQLJSONIndexConsistencyDetectsUnbuiltAndCorruptIndex(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"team":"blue"},{"team":"blue"},{"name":"missing"}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "team"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}

	report, available, err := trie.CheckSQLJSONIndexConsistency("people")
	if err != nil || !available {
		t.Fatalf("initial CheckSQLJSONIndexConsistency() = %#v, %v, %v", report, available, err)
	}
	if report.Consistent || len(report.Indexes) != 1 || report.Indexes[0].Ready {
		t.Fatalf("initial consistency report = %#v, want one unready inconsistent index", report)
	}

	if _, ok, err := trie.SQLJSONIndexHealth("people", "team"); err != nil || !ok {
		t.Fatalf("SQLJSONIndexHealth() = %v, %v", ok, err)
	}
	report, available, err = trie.CheckSQLJSONIndexConsistency("people")
	if err != nil || !available || !report.Consistent || len(report.Indexes) != 1 || !report.Indexes[0].Consistent {
		t.Fatalf("healthy consistency report = %#v, %v, %v", report, available, err)
	}

	for valueKey := range trie.sqlJSONIndexes["people"]["team"].rows {
		delete(trie.sqlJSONIndexes["people"]["team"].rows, valueKey)
		break
	}
	report, available, err = trie.CheckSQLJSONIndexConsistency("people")
	if err != nil || !available || report.Consistent || report.Indexes[0].Consistent {
		t.Fatalf("corrupt consistency report = %#v, %v, %v", report, available, err)
	}
}
