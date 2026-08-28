package hatCache

import "testing"

func TestCheckAndRepairIntegrityValidateLiveTypedStorage(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("string", "value")
	trie.UpsertMap("map", map[string]interface{}{"count": int64(7)})
	if report, err := trie.CheckIntegrity(); err != nil || report.Entries != 2 {
		t.Fatalf("CheckIntegrity() = %#v, %v", report, err)
	}
	repair, err := trie.RepairIntegrity()
	if err != nil || repair.After.Entries != 2 {
		t.Fatalf("RepairIntegrity() = %#v, %v", repair, err)
	}
}
