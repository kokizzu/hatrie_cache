package hatSql

import "testing"

func TestCH005PatchStateManifestMatchesOneSnapshot(t *testing.T) {
	table := ch005NewPatchTable(t, "events")
	for _, key := range []string{"a", "b", "c"} {
		if _, err := table.Upsert(key, []TypedTableValue{TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.Delete("b"); err != nil {
		t.Fatal(err)
	}
	snapshot, bitmap, err := table.MarshalPatchStateWithManifest()
	if err != nil {
		t.Fatalf("MarshalPatchStateWithManifest() error = %v", err)
	}
	if bitmap.RowCount != 3 || bitmap.DeletedCount != 1 {
		t.Fatalf("bitmap counts = %#v, want 3 rows/1 deleted", bitmap)
	}
	if err := bitmap.Verify(snapshot); err != nil {
		t.Fatalf("bitmap verification error = %v", err)
	}
}
