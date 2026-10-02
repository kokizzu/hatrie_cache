package hatDataStructure

import "testing"

func benchmarkTupleMigrationFormats(b *testing.B) (TupleFormat, TupleFormat, TupleFormat) {
	b.Helper()
	v1, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "region", Type: TupleFieldString},
	})
	if err != nil {
		b.Fatal(err)
	}
	v2, err := NewTupleFormat(2, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "region", Type: TupleFieldString},
		{Name: "state", Type: TupleFieldString},
	})
	if err != nil {
		b.Fatal(err)
	}
	v3, err := NewTupleFormat(3, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "region", Type: TupleFieldString},
		{Name: "state", Type: TupleFieldString},
		{Name: "score", Type: TupleFieldInt64},
	})
	if err != nil {
		b.Fatal(err)
	}
	return v1, v2, v3
}

func BenchmarkTupleMigrationManual(b *testing.B) {
	_, v2, v3 := benchmarkTupleMigrationFormats(b)
	v2Values := []TupleFieldValue{TupleInt64(42), TupleString("apac"), TupleString("active")}
	v3Values := []TupleFieldValue{TupleInt64(42), TupleString("apac"), TupleString("active"), TupleInt64(7)}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		mid, err := NewVersionedTuple(v2, v2Values)
		if err != nil {
			b.Fatal(err)
		}
		final, err := NewVersionedTuple(v3, v3Values)
		if err != nil {
			b.Fatal(err)
		}
		if mid.Version() == 0 || final.Version() == 0 {
			b.Fatal("unexpected zero version")
		}
	}
}

func BenchmarkTupleMigrationPlan(b *testing.B) {
	v1, v2, v3 := benchmarkTupleMigrationFormats(b)
	source, err := NewVersionedTuple(v1, []TupleFieldValue{TupleInt64(42), TupleString("apac")})
	if err != nil {
		b.Fatal(err)
	}
	plan, err := NewTupleMigrationPlan("accounts", []TupleMigrationStep{
		{
			From: v1,
			To:   v2,
			Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				return NewVersionedTuple(destination, []TupleFieldValue{TupleInt64(42), TupleString("apac"), TupleString("active")})
			},
			Rollback: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				return NewVersionedTuple(destination, []TupleFieldValue{TupleInt64(42), TupleString("apac")})
			},
		},
		{
			From: v2,
			To:   v3,
			Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				return NewVersionedTuple(destination, []TupleFieldValue{TupleInt64(42), TupleString("apac"), TupleString("active"), TupleInt64(7)})
			},
			Rollback: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
				return NewVersionedTuple(destination, []TupleFieldValue{TupleInt64(42), TupleString("apac"), TupleString("active")})
			},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		migrated, err := plan.Migrate(source, v3.Version())
		if err != nil {
			b.Fatal(err)
		}
		if migrated.Version() != v3.Version() {
			b.Fatal("unexpected migration version")
		}
	}
}
