package hatDataStructure

import "testing"

func BenchmarkTT017VersionedTupleSpaceUpgrade(b *testing.B) {
	const recordCount = 1024

	b.Run("space_upgrade", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			manager, oldTuples := benchmarkTT017Fixture(b, recordCount)
			space, err := NewVersionedTupleSpace(manager)
			if err != nil {
				b.Fatal(err)
			}
			for index, tuple := range oldTuples {
				if err := space.Restore(benchmarkTT017Key(index), tuple); err != nil {
					b.Fatal(err)
				}
			}
			status, err := space.StartUpgrade()
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			for status.Active {
				status, err = space.UpgradeStep(128)
				if err != nil {
					b.Fatal(err)
				}
			}
			if !status.Complete || status.Pending != 0 {
				b.Fatalf("upgrade status = %#v", status)
			}
		}
	})

	b.Run("direct_migrate", func(b *testing.B) {
		b.ReportAllocs()
		manager, oldTuples := benchmarkTT017Fixture(b, recordCount)
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for _, tuple := range oldTuples {
				if _, err := manager.MigrateTo(tuple, 2); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func benchmarkTT017Fixture(b *testing.B, count int) (*VersionedTupleMigrationManager, []VersionedTuple) {
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
	manager, err := NewVersionedTupleMigrationManager(formatV2)
	if err != nil {
		b.Fatal(err)
	}
	if err := manager.RegisterFormat(formatV1); err != nil {
		b.Fatal(err)
	}
	if err := manager.RegisterMigration(1, 2, func(values []TupleFieldValue) ([]TupleFieldValue, error) {
		return append(values, TupleBool(false)), nil
	}); err != nil {
		b.Fatal(err)
	}
	oldTuples := make([]VersionedTuple, count)
	for index := range oldTuples {
		oldTuples[index], err = formatV1.PackVersioned([]TupleFieldValue{
			TupleInt64(int64(index)),
			TupleString("value"),
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	return manager, oldTuples
}

func benchmarkTT017Key(index int) string {
	return "key-" + benchmarkTT017Uint(index)
}

func benchmarkTT017Uint(value int) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var buffer [20]byte
	position := len(buffer)
	for value > 0 {
		position--
		buffer[position] = digits[value%10]
		value /= 10
	}
	return string(buffer[position:])
}
