package hatDataStructure

import "testing"

var versionedTupleSpaceBaselineSink VersionedTuple

// BenchmarkVersionedTupleSpaceBaseline measures the current explicit
// unpack-transform-repack migration path before the online migration manager.
func BenchmarkVersionedTupleSpaceBaseline(b *testing.B) {
	defaultActive := TupleBool(true)
	formatV1, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "payload", Type: TupleFieldBytes},
	})
	if err != nil {
		b.Fatal(err)
	}
	formatV2, err := NewTupleFormat(2, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "payload", Type: TupleFieldBytes},
		{Name: "active", Type: TupleFieldBool, Default: &defaultActive},
	})
	if err != nil {
		b.Fatal(err)
	}
	original, err := formatV1.PackVersioned([]TupleFieldValue{
		TupleInt64(42),
		TupleString("orders"),
		TupleBytes([]byte("payload")),
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values, err := formatV1.Unpack(original.Tuple())
		if err != nil {
			b.Fatal(err)
		}
		migrated, err := formatV2.PackVersioned(values)
		if err != nil {
			b.Fatal(err)
		}
		versionedTupleSpaceBaselineSink = migrated
	}
}
