package hatCache

import "testing"

func TestSQLSecondaryIndexesRefreshAfterStringReplacement(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1,"state":"queued","body":"fast cache","team":"core","enabled":true}]`)
	if err := trie.CreateSQLJSONBitmapIndex("jobs", "state"); err != nil {
		t.Fatal(err)
	}
	if err := trie.CreateSQLJSONTextIndex("jobs", "body"); err != nil {
		t.Fatal(err)
	}
	if err := trie.CreateSQLJSONCompositeIndex("jobs", "team", "enabled"); err != nil {
		t.Fatal(err)
	}

	if _, available, err := trie.ResolveSQLIndexedSource("CACHE", "jobs", "state", "queued"); err != nil || !available {
		t.Fatalf("initial bitmap lookup available/error = %t/%v", available, err)
	}
	if _, available, err := trie.ResolveSQLTextSource("CACHE", "jobs", "body", "fast"); err != nil || !available {
		t.Fatalf("initial text lookup available/error = %t/%v", available, err)
	}
	if _, available, err := trie.ResolveSQLCompositeIndexedSource("CACHE", "jobs", []string{"team", "enabled"}, []interface{}{"core", true}); err != nil || !available {
		t.Fatalf("initial composite lookup available/error = %t/%v", available, err)
	}

	trie.UpsertString("jobs", `[{"id":2,"state":"running","body":"reliable transfer","team":"edge","enabled":false}]`)
	bitmapRows, available, err := trie.ResolveSQLIndexedSource("CACHE", "jobs", "state", "running")
	if err != nil || !available || len(bitmapRows) != 1 || bitmapRows[0]["id"] != float64(2) {
		t.Fatalf("replacement bitmap rows/available/error = %#v/%t/%v", bitmapRows, available, err)
	}
	textRows, available, err := trie.ResolveSQLTextSource("CACHE", "jobs", "body", "reliable")
	if err != nil || !available || len(textRows) != 1 || textRows[0]["id"] != float64(2) {
		t.Fatalf("replacement text rows/available/error = %#v/%t/%v", textRows, available, err)
	}
	compositeRows, available, err := trie.ResolveSQLCompositeIndexedSource("CACHE", "jobs", []string{"team", "enabled"}, []interface{}{"edge", false})
	if err != nil || !available || len(compositeRows) != 1 || compositeRows[0]["id"] != float64(2) {
		t.Fatalf("replacement composite rows/available/error = %#v/%t/%v", compositeRows, available, err)
	}
}
