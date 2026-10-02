package hatDataStructure

import (
	"context"
	"testing"
)

var onlineTupleUpgradeBenchmarkSink VersionedTuple

func benchmarkOnlineTupleUpgradeFixture(b *testing.B) (VersionedTuple, TupleFormat, *TupleMigrationPlan) {
	b.Helper()
	v1, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
	})
	if err != nil {
		b.Fatal(err)
	}
	v2, err := NewTupleFormat(2, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "state", Type: TupleFieldString},
	})
	if err != nil {
		b.Fatal(err)
	}
	source, err := NewVersionedTuple(v1, []TupleFieldValue{TupleInt64(42), TupleString("alice")})
	if err != nil {
		b.Fatal(err)
	}
	plan, err := NewTupleMigrationPlan("accounts", []TupleMigrationStep{{
		From: v1,
		To:   v2,
		Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
			return NewVersionedTuple(destination, []TupleFieldValue{TupleInt64(42), TupleString("alice"), TupleString("active")})
		},
		Rollback: func(tuple VersionedTuple, source TupleFormat) (VersionedTuple, error) {
			return NewVersionedTuple(source, []TupleFieldValue{TupleInt64(42), TupleString("alice")})
		},
	}})
	if err != nil {
		b.Fatal(err)
	}
	return source, v1, plan
}

func BenchmarkOnlineTupleUpgradeManualWrite(b *testing.B) {
	source, _, plan := benchmarkOnlineTupleUpgradeFixture(b)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		updated, err := plan.Migrate(source, 2)
		if err != nil {
			b.Fatal(err)
		}
		onlineTupleUpgradeBenchmarkSink = updated
	}
}

func BenchmarkOnlineTupleUpgradeWrite(b *testing.B) {
	source, format, plan := benchmarkOnlineTupleUpgradeFixture(b)
	upgrade, err := NewOnlineTupleUpgrade(plan, format.Version(), 2)
	if err != nil {
		b.Fatal(err)
	}
	if err := upgrade.Begin(); err != nil {
		b.Fatal(err)
	}
	store := newOnlineTupleUpgradeTestStore(nil)
	ctx := context.Background()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if err := upgrade.Write(ctx, store, []byte("account:42"), source); err != nil {
			b.Fatal(err)
		}
	}
	stored, _, err := store.Get(ctx, []byte("account:42"))
	if err != nil {
		b.Fatal(err)
	}
	onlineTupleUpgradeBenchmarkSink = stored
}
