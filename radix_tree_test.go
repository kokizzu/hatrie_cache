package hatriecache

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestRadixTreePutGetDeleteAndPrefix(t *testing.T) {
	tree := newRadixTreeData()
	nested := Map{"role": "admin"}
	if !tree.Put("user:100/profile", nested) {
		t.Fatal("Put(user:100/profile) = false, want new insert")
	}
	nested["role"] = "caller"
	if !tree.Put("user:100/session", "active") {
		t.Fatal("Put(user:100/session) = false, want new insert")
	}
	if !tree.Put("user:101/profile", "viewer") {
		t.Fatal("Put(user:101/profile) = false, want new insert")
	}
	if tree.Put("user:100/session", "idle") {
		t.Fatal("Put(existing) = true, want replacement")
	}

	value, ok := tree.Get("user:100/profile")
	if !ok || !reflect.DeepEqual(value, Map{"role": "admin"}) {
		t.Fatalf("Get(user:100/profile) = %#v/%v, want stored clone", value, ok)
	}
	value.(Map)["role"] = "reader"
	value, ok = tree.Get("user:100/profile")
	if !ok || value.(Map)["role"] != "admin" {
		t.Fatalf("Get after caller mutation = %#v/%v, want unchanged clone", value, ok)
	}
	if value, ok := tree.Get("user:100/session"); !ok || value != "idle" {
		t.Fatalf("Get(user:100/session) = %#v/%v, want replacement", value, ok)
	}

	items := tree.ItemsWithPrefix("user:100/")
	if got, want := radixTreeItemKeys(items), []string{"user:100/profile", "user:100/session"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ItemsWithPrefix(user:100/) keys = %#v, want %#v", got, want)
	}
	if !tree.Delete("user:100/profile") {
		t.Fatal("Delete(user:100/profile) = false, want true")
	}
	if tree.Delete("user:100/profile") {
		t.Fatal("Delete(user:100/profile again) = true, want false")
	}
	if _, ok := tree.Get("user:100/profile"); ok {
		t.Fatal("deleted key still present")
	}
	if value, ok := tree.Get("user:100/session"); !ok || value != "idle" {
		t.Fatalf("Get(user:100/session after delete) = %#v/%v, want retained sibling", value, ok)
	}

	info := tree.Info()
	if info.Items != 2 || info.Nodes == 0 || info.Edges == 0 || info.LabelBytes == 0 || info.EncodedBytes == 0 {
		t.Fatalf("Info() = %#v, want compact populated tree", info)
	}
}

func TestRadixTreeSnapshotRoundTrip(t *testing.T) {
	tree := newRadixTreeData()
	tree.Put("asset:css", "main.css")
	tree.Put("asset:js", json.Number("42"))
	tree.Put("session", Map{"status": "active"})

	snapshot := tree.Snapshot()
	if got, want := radixTreeItemKeys(snapshot.Items), []string{"asset:css", "asset:js", "session"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() keys = %#v, want %#v", got, want)
	}
	restored, err := newRadixTreeDataFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("newRadixTreeDataFromSnapshot() error = %v", err)
	}
	if got := restored.ItemsWithPrefix("asset:"); !reflect.DeepEqual(radixTreeItemKeys(got), []string{"asset:css", "asset:js"}) {
		t.Fatalf("restored asset prefix keys = %#v, want asset keys", got)
	}
	if value, ok := restored.Get("asset:js"); !ok || value != json.Number("42") {
		t.Fatalf("restored Get(asset:js) = %#v/%v, want json.Number", value, ok)
	}
}

func TestRadixTreeSnapshotValidationRejectsCorruptPayload(t *testing.T) {
	if err := validateRadixTreeSnapshot(radixTreeSnapshot{
		Count: 1,
		Items: []RadixTreeItem{},
	}); err == nil {
		t.Fatal("validateRadixTreeSnapshot(count mismatch) error = nil, want error")
	}
	if err := validateRadixTreeSnapshot(radixTreeSnapshot{
		Count: 2,
		Items: []RadixTreeItem{
			{Key: "b", Value: "second"},
			{Key: "a", Value: "first"},
		},
	}); err == nil {
		t.Fatal("validateRadixTreeSnapshot(unsorted) error = nil, want error")
	}
	if err := validateRadixTreeSnapshot(radixTreeSnapshot{
		Count: 2,
		Items: []RadixTreeItem{
			{Key: "a", Value: "first"},
			{Key: "a", Value: "duplicate"},
		},
	}); err == nil {
		t.Fatal("validateRadixTreeSnapshot(duplicate) error = nil, want error")
	}
}

func TestHatTrieRadixTreeOperations(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertRadixTree("index")
	if hval := ht.Get("index"); !hval.IsRadixTree() {
		t.Fatalf("UpsertRadixTree stored type %+v, want radix tree", hval)
	}
	idx := ht.Get("index").Index
	if added := ht.PutRadixTree("index", "user:100", "active"); !added {
		t.Fatal("PutRadixTree(new) = false, want true")
	}
	if added := ht.PutRadixTree("index", "user:101", "idle"); !added {
		t.Fatal("PutRadixTree(second new) = false, want true")
	}
	if added := ht.PutRadixTree("index", "user:101", "away"); added {
		t.Fatal("PutRadixTree(existing) = true, want false")
	}
	if value, ok := ht.GetRadixTree("index", "user:101"); !ok || value != "away" {
		t.Fatalf("GetRadixTree(user:101) = %#v/%v, want away", value, ok)
	}
	if !ht.HasRadixTree("index", "user:100") {
		t.Fatal("HasRadixTree(user:100) = false, want true")
	}
	items, ok := ht.ScanRadixTree("index", "user:")
	if !ok || !reflect.DeepEqual(radixTreeItemKeys(items), []string{"user:100", "user:101"}) {
		t.Fatalf("ScanRadixTree(user:) = %#v/%v, want two sorted users", items, ok)
	}
	if !ht.DeleteRadixTree("index", "user:100") {
		t.Fatal("DeleteRadixTree(user:100) = false, want true")
	}
	if ht.HasRadixTree("index", "user:100") {
		t.Fatal("HasRadixTree(user:100 after delete) = true, want false")
	}
	info, ok := ht.RadixTreeInfo("index")
	if !ok || info.Items != 1 || info.Nodes == 0 {
		t.Fatalf("RadixTreeInfo(index) = %#v/%v, want one item", info, ok)
	}

	ht.UpsertRadixTree("index")
	if got := ht.Get("index"); !got.IsRadixTree() || got.Index != idx {
		t.Fatalf("UpsertRadixTree replacement stored %+v, want same radix tree slot %d", got, idx)
	}
	if added := ht.PutRadixTree("auto", "key", "value"); !added {
		t.Fatal("PutRadixTree(auto) = false, want true")
	}
	if !ht.Get("auto").IsRadixTree() {
		t.Fatal("PutRadixTree on missing key did not create a radix tree")
	}
}

func TestRadixTreeStorageReleasedOnOverwrite(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertRadixTree("index")
	idx := ht.Get("index").Index
	ht.UpsertString("index", "replacement")
	if !radixTreeIndexReleased(ht, idx) {
		t.Fatalf("overwritten radix tree index %d was not released", idx)
	}
	ht.UpsertRadixTree("new")
	if got := ht.Get("new").Index; got != idx {
		t.Fatalf("radix tree storage was not reused: got index %d, want %d", got, idx)
	}
}

func radixTreeIndexReleased(ht *HatTrie, idx int32) bool {
	return int(idx) >= len(ht.radixTrees.array) || ht.radixTrees.reusables.Has(idx)
}

func radixTreeItemKeys(items []RadixTreeItem) []string {
	keys := make([]string, len(items))
	for idx, item := range items {
		keys[idx] = item.Key
	}
	return keys
}
