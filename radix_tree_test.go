package hatriecache

import (
	"encoding/json"
	"fmt"
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

func TestRadixTreePrefixScanUsesBoundedCapacity(t *testing.T) {
	tree := newRadixTreeData()
	for idx := 0; idx < 200; idx++ {
		tree.Put(fmt.Sprintf("cold:%03d", idx), idx)
	}
	tree.Put("hot:one", "value")

	items := tree.ItemsWithPrefix("hot:")
	if got, want := radixTreeItemKeys(items), []string{"hot:one"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ItemsWithPrefix(hot:) keys = %#v, want %#v", got, want)
	}
	if cap(items) > maxRadixTreePrefixScanCapacity {
		t.Fatalf("ItemsWithPrefix(hot:) capacity = %d, want at most %d", cap(items), maxRadixTreePrefixScanCapacity)
	}

	snapshot := tree.Snapshot()
	if len(snapshot.Items) != 201 {
		t.Fatalf("Snapshot() items = %d, want 201", len(snapshot.Items))
	}
	if cap(snapshot.Items) != len(snapshot.Items) {
		t.Fatalf("Snapshot() capacity = %d, want exact full-scan capacity %d", cap(snapshot.Items), len(snapshot.Items))
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
	if err := validateRadixTreeSnapshot(radixTreeSnapshot{
		Count: 1,
		Items: []RadixTreeItem{{Key: "a", Value: func() {}}},
	}); err == nil {
		t.Fatal("validateRadixTreeSnapshot(unsupported value) error = nil, want error")
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

func TestCheckedRadixTreeOperationsReturnValuesAndCopies(t *testing.T) {
	ht := newTestTrie(t)

	added, err := ht.PutRadixTreeChecked("index", "user:100/profile", Map{"status": "active"})
	if err != nil || !added {
		t.Fatalf("PutRadixTreeChecked(new) = %v/%v, want true/nil", added, err)
	}
	addedCount, err := ht.PutRadixTreeEntriesChecked("index", Map{
		"asset:logo":       []byte("png"),
		"user:101/profile": "pending",
	})
	if err != nil || addedCount != 2 {
		t.Fatalf("PutRadixTreeEntriesChecked(new) = %d/%v, want 2/nil", addedCount, err)
	}

	value, ok, err := ht.GetRadixTreeChecked("index", "user:100/profile")
	if err != nil || !ok || !reflect.DeepEqual(value, Map{"status": "active"}) {
		t.Fatalf("GetRadixTreeChecked(user:100/profile) = %#v/%v/%v, want stored map", value, ok, err)
	}
	value.(Map)["status"] = "changed"
	if again, ok, err := ht.GetRadixTreeChecked("index", "user:100/profile"); err != nil || !ok || again.(Map)["status"] != "active" {
		t.Fatalf("GetRadixTreeChecked(after caller mutation) = %#v/%v/%v, want original map", again, ok, err)
	}

	items, ok, err := ht.ScanRadixTreeChecked("index", "user:")
	if err != nil || !ok || !reflect.DeepEqual(radixTreeItemKeys(items), []string{"user:100/profile", "user:101/profile"}) {
		t.Fatalf("ScanRadixTreeChecked(user:) = %#v/%v/%v, want sorted user keys", items, ok, err)
	}
	items[0].Value.(Map)["status"] = "scan"
	if again, ok, err := ht.GetRadixTreeChecked("index", "user:100/profile"); err != nil || !ok || again.(Map)["status"] != "active" {
		t.Fatalf("GetRadixTreeChecked(after scan mutation) = %#v/%v/%v, want original map", again, ok, err)
	}

	hit, err := ht.HasRadixTreeChecked("index", "asset:logo")
	if err != nil || !hit {
		t.Fatalf("HasRadixTreeChecked(asset:logo) = %v/%v, want true/nil", hit, err)
	}
	info, ok, err := ht.RadixTreeInfoChecked("index")
	if err != nil || !ok || info.Items != 3 || info.Nodes == 0 {
		t.Fatalf("RadixTreeInfoChecked(index) = %#v/%v/%v, want populated tree", info, ok, err)
	}
	deleted, err := ht.DeleteRadixTreeChecked("index", "asset:logo")
	if err != nil || !deleted {
		t.Fatalf("DeleteRadixTreeChecked(asset:logo) = %v/%v, want true/nil", deleted, err)
	}
	hit, err = ht.HasRadixTreeChecked("index", "asset:logo")
	if err != nil || hit {
		t.Fatalf("HasRadixTreeChecked(asset:logo after delete) = %v/%v, want false/nil", hit, err)
	}

	ht.UpsertString("string", "value")
	added, err = ht.PutRadixTreeChecked("string", "key", "value")
	if err != nil || !added {
		t.Fatalf("PutRadixTreeChecked(overwrite string) = %v/%v, want true/nil", added, err)
	}
	if hval := ht.Get("string"); !hval.IsRadixTree() {
		t.Fatalf("PutRadixTreeChecked(overwrite string) stored %+v, want radix tree", hval)
	}
	if value, ok, err := ht.GetRadixTreeChecked("missing", "key"); err != nil || ok || value != nil {
		t.Fatalf("GetRadixTreeChecked(missing) = %#v/%v/%v, want nil/false/nil", value, ok, err)
	}
	if items, ok, err := ht.ScanRadixTreeChecked("missing", ""); err != nil || ok || items != nil {
		t.Fatalf("ScanRadixTreeChecked(missing) = %#v/%v/%v, want nil/false/nil", items, ok, err)
	}
}

func TestCheckedRadixTreeRejectsUnsupportedValues(t *testing.T) {
	ht := newTestTrie(t)
	unsupported := func() {}

	if added, err := ht.PutRadixTreeChecked("index", "bad", unsupported); err == nil || added {
		t.Fatalf("PutRadixTreeChecked(unsupported missing) = %v/%v, want false/error", added, err)
	}
	if hval := ht.Get("index"); !hval.Empty() {
		t.Fatalf("PutRadixTreeChecked(unsupported missing) stored value %+v", hval)
	}

	if added, err := ht.PutRadixTreeChecked("index", "keep", "value"); err != nil || !added {
		t.Fatalf("PutRadixTreeChecked(keep) = %v/%v, want true/nil", added, err)
	}
	if added, err := ht.PutRadixTreeChecked("index", "bad", unsupported); err == nil || added {
		t.Fatalf("PutRadixTreeChecked(unsupported existing) = %v/%v, want false/error", added, err)
	}
	if got, ok, err := ht.GetRadixTreeChecked("index", "keep"); err != nil || !ok || got != "value" {
		t.Fatalf("GetRadixTreeChecked(keep after rejected put) = %#v/%v/%v, want value/true/nil", got, ok, err)
	}
	if hit, err := ht.HasRadixTreeChecked("index", "bad"); err != nil || hit {
		t.Fatalf("HasRadixTreeChecked(bad after rejected put) = %v/%v, want false/nil", hit, err)
	}

	if added, err := ht.PutRadixTreeEntriesChecked("index", Map{"new": "value", "bad": unsupported}); err == nil || added != 0 {
		t.Fatalf("PutRadixTreeEntriesChecked(unsupported) = %d/%v, want 0/error", added, err)
	}
	if hit, err := ht.HasRadixTreeChecked("index", "new"); err != nil || hit {
		t.Fatalf("HasRadixTreeChecked(new after rejected batch) = %v/%v, want false/nil", hit, err)
	}
	if info, ok, err := ht.RadixTreeInfoChecked("index"); err != nil || !ok || info.Items != 1 {
		t.Fatalf("RadixTreeInfoChecked(after rejected batch) = %#v/%v/%v, want one item", info, ok, err)
	}

	if added := ht.PutRadixTree("unchecked", "bad", unsupported); !added {
		t.Fatal("PutRadixTree unchecked unsupported value = false, want legacy permissive insert")
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
