package hatDataStructure

import "testing"

var versionedTupleMigrationBenchmarkSink VersionedTuple

func BenchmarkTR016VersionedTupleMigration(b *testing.B) {
	formatV1, formatV2, formatV3 := mustTR016BenchmarkFormats(b)
	manager, err := NewVersionedTupleMigrationManager(formatV3)
	if err != nil {
		b.Fatal(err)
	}
	if err := manager.RegisterFormat(formatV1); err != nil {
		b.Fatal(err)
	}
	if err := manager.RegisterFormat(formatV2); err != nil {
		b.Fatal(err)
	}
	if err := manager.RegisterMigration(1, 2, func(values []TupleFieldValue) ([]TupleFieldValue, error) {
		return []TupleFieldValue{values[0], TupleString(values[1].String + "-migrated")}, nil
	}); err != nil {
		b.Fatal(err)
	}
	if err := manager.RegisterMigration(2, 3, func(values []TupleFieldValue) ([]TupleFieldValue, error) {
		return append(values, TupleString("ap-southeast")), nil
	}); err != nil {
		b.Fatal(err)
	}
	original, err := formatV1.PackVersioned([]TupleFieldValue{TupleInt64(42), TupleString("orders")})
	if err != nil {
		b.Fatal(err)
	}

	b.Run("manager", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			migrated, err := manager.Migrate(original)
			if err != nil {
				b.Fatal(err)
			}
			versionedTupleMigrationBenchmarkSink = migrated
		}
	})
	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			values, err := formatV1.Unpack(original.Tuple())
			if err != nil {
				b.Fatal(err)
			}
			values, err = []TupleFieldValue{values[0], TupleString(values[1].String + "-migrated")}, nil
			if err != nil {
				b.Fatal(err)
			}
			intermediate, err := formatV2.PackVersioned(values)
			if err != nil {
				b.Fatal(err)
			}
			values, err = formatV2.Unpack(intermediate.Tuple())
			if err != nil {
				b.Fatal(err)
			}
			values = append(values, TupleString("ap-southeast"))
			migrated, err := formatV3.PackVersioned(values)
			if err != nil {
				b.Fatal(err)
			}
			versionedTupleMigrationBenchmarkSink = migrated
		}
	})
}

func mustTR016BenchmarkFormats(b testing.TB) (TupleFormat, TupleFormat, TupleFormat) {
	b.Helper()
	formatV1, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
	})
	if err != nil {
		b.Fatal(err)
	}
	activeDefault := TupleBool(false)
	formatV2, err := NewTupleFormat(2, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "active", Type: TupleFieldBool, Default: &activeDefault},
	})
	if err != nil {
		b.Fatal(err)
	}
	formatV3, err := NewTupleFormat(3, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "active", Type: TupleFieldBool},
		{Name: "region", Type: TupleFieldString},
	})
	if err != nil {
		b.Fatal(err)
	}
	return formatV1, formatV2, formatV3
}
