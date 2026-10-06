package hatSql

import "testing"

func TestC212JoinHashIndexKeepsSingletonInlineBeforeGrowing(t *testing.T) {
	index := newSQLJoinHashIndex(2)
	key, ok := newSQLJoinProbeKey(int64(42))
	if !ok {
		t.Fatal("numeric probe key was rejected")
	}
	index.addKey(key, 7)

	bucket, found := index.lookupBucket(key)
	if !found {
		t.Fatal("singleton bucket was not found")
	}
	if bucket.isDuplicate() || bucket.row() != 7 {
		t.Fatalf("singleton bucket = %#v, want inline row 7", bucket)
	}
	if rows := index.duplicateRowsFor(bucket); rows != nil {
		t.Fatalf("singleton bucket allocated duplicate rows: %#v", rows)
	}

	index.addKey(key, 11)
	bucket, found = index.lookupBucket(key)
	if !found || !bucket.isDuplicate() {
		t.Fatalf("duplicate bucket = %#v, found=%v, want duplicate rows", bucket, found)
	}
	assertC212JoinIndexRows(t, index.duplicateRowsFor(bucket), []int{7, 11})
}
