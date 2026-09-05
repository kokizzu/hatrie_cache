package hatSql

import (
	"math"
	"testing"
)

func TestTypedTableAggregateGroupHashAndEquality(t *testing.T) {
	groupBy := []int{0, 1}
	values := []TypedTableValue{TypedString("west"), TypedInt64(42)}
	if got, want := typedTableAggregateGroupHash(values, groupBy), typedTableAggregateGroupHash(values, groupBy); got != want {
		t.Fatalf("same group hash = %d, want %d", got, want)
	}
	if !typedTableAggregateGroupValuesEqual(values, values, groupBy) {
		t.Fatal("same typed group values are not equal")
	}
	if typedTableAggregateGroupValuesEqual(values, []TypedTableValue{TypedString("east"), TypedInt64(42)}, groupBy) {
		t.Fatal("different string group values compare equal")
	}
	if typedTableAggregateGroupValuesEqual(values, []TypedTableValue{TypedString("west"), TypedInt64(43)}, groupBy) {
		t.Fatal("different numeric group values compare equal")
	}
}

func TestTypedTableAggregateGroupEqualityPreservesFloatIdentity(t *testing.T) {
	groupBy := []int{0}
	negativeZero := []TypedTableValue{TypedFloat64(math.Copysign(0, -1))}
	positiveZero := []TypedTableValue{TypedFloat64(0)}
	if typedTableAggregateGroupValuesEqual(negativeZero, positiveZero, groupBy) {
		t.Fatal("negative zero and positive zero compare equal")
	}

	nanBits := uint64(0x7ff8000000000042)
	firstNaN := []TypedTableValue{{Kind: TypedTableFloat64, Float64: math.Float64frombits(nanBits), Valid: true}}
	secondNaN := []TypedTableValue{{Kind: TypedTableFloat64, Float64: math.Float64frombits(nanBits), Valid: true}}
	if !typedTableAggregateGroupValuesEqual(firstNaN, secondNaN, groupBy) {
		t.Fatal("same-bit NaN values are not equal")
	}
}

func TestTypedTableAggregateLegacyGroupKeyRemainsStable(t *testing.T) {
	values := []TypedTableValue{
		TypedString("a|b"),
		TypedInt64(-7),
		TypedFloat64(1.5),
		TypedBool(true),
	}
	key := typedTableAggregateLegacyGroupKey(values, []int{0, 1, 2, 3})
	if want := "\x013:a|b|\x02-7|\x033ff8000000000000|\x041|"; key != want {
		t.Fatalf("legacy group key = %q, want %q", key, want)
	}
}

func TestTypedTableAggregateDeleteCollisionPreservesPrimary(t *testing.T) {
	const hash = uint64(7)
	aggregate := &TypedTableAggregate{
		groups: map[uint64]typedTableAggregateGroupBucket{
			hash: {
				group:      typedTableAggregateGroup{key: "primary"},
				collisions: []typedTableAggregateGroup{{key: "collision"}},
			},
		},
		groupCount: 2,
	}

	aggregate.deleteGroup(hash, aggregate.groups[hash], 1)
	bucket, ok := aggregate.groups[hash]
	if !ok {
		t.Fatal("deleting the collision removed the primary group")
	}
	if bucket.group.key != "primary" || len(bucket.collisions) != 0 {
		t.Fatalf("remaining bucket = %#v, want primary without collisions", bucket)
	}
	if aggregate.groupCount != 1 {
		t.Fatalf("group count = %d, want 1", aggregate.groupCount)
	}
}
