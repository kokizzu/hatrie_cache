package hatSql

import "testing"

func TestImportDeduplicationAndTableDiff(t *testing.T) {
	dedup := NewImportDeduplicator()
	if duplicate := dedup.Seen("feed", 10, []byte("row")); duplicate {
		t.Fatal("first import marked duplicate")
	}
	if duplicate := dedup.Seen("feed", 10, []byte("row")); !duplicate {
		t.Fatal("same offset/hash not deduplicated")
	}
	diff := DiffExternalTables(ExternalTable{Columns: []string{"id"}, Rows: []Row{{"id": 1}}}, ExternalTable{Columns: []string{"id", "name"}, Rows: []Row{{"id": 2}}})
	if len(diff.AddedColumns) != 1 || len(diff.RemovedRows) != 1 || len(diff.AddedRows) != 1 {
		t.Fatalf("diff = %#v", diff)
	}
}
