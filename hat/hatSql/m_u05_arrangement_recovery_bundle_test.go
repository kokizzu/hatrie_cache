package hatSql_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableArrangementRecoveryRoundTripsAndRestoresAll(t *testing.T) {
	aggregateTable, aggregateChanges, aggregateDefinition := mU05AggregateTable(t)
	aggregates, err := hatSql.NewTypedTableAggregateArrangements(aggregateTable)
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := aggregates.Acquire(aggregateDefinition)
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply(aggregateChanges); err != nil {
		t.Fatal(err)
	}
	aggregateRows := aggregate.Rows()

	left, leftChanges := mU05JoinTable(t, "left", []string{"left-red", "left-blue"})
	right, rightChanges := mU05JoinTable(t, "right", []string{"right-red"})
	joins, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	joinDefinition := hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}
	join, err := joins.Acquire(joinDefinition)
	if err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyLeft(leftChanges); err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyRight(rightChanges); err != nil {
		t.Fatal(err)
	}
	joinRows := join.Rows()

	checkpoint, err := hatSql.CaptureTypedTableArrangementRecovery(
		[]*hatSql.TypedTableAggregateArrangements{aggregates},
		[]*hatSql.TypedTableJoinArrangements{joins},
	)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	var decoded hatSql.TypedTableArrangementRecoveryCheckpoint
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, checkpoint) {
		t.Fatalf("checkpoint round trip = %#v, want %#v", decoded, checkpoint)
	}
	if len(decoded.Aggregates) != 1 || len(decoded.Joins) != 1 {
		t.Fatalf("checkpoint counts = %d aggregates, %d joins", len(decoded.Aggregates), len(decoded.Joins))
	}
	for _, group := range checkpoint.Aggregates[0].Groups {
		if group.Key == "" {
			t.Fatal("captured aggregate group key is empty")
		}
	}
	repeated, err := hatSql.CaptureTypedTableArrangementRecovery(
		[]*hatSql.TypedTableAggregateArrangements{aggregates},
		[]*hatSql.TypedTableJoinArrangements{joins},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(repeated, checkpoint) {
		t.Fatalf("repeated capture = %#v, want %#v", repeated, checkpoint)
	}

	aggregate.Release()
	join.Release()
	recoveredAggregates, err := hatSql.NewTypedTableAggregateArrangements(aggregateTable)
	if err != nil {
		t.Fatal(err)
	}
	recoveredJoins, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	recoveryLease, err := hatSql.RestoreTypedTableArrangementRecovery(
		[]*hatSql.TypedTableAggregateArrangements{recoveredAggregates},
		[]*hatSql.TypedTableJoinArrangements{recoveredJoins},
		decoded,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := recoveryLease.AggregateArrangements()[0].Rows(); !reflect.DeepEqual(got, aggregateRows) {
		t.Fatalf("recovered aggregate rows = %#v, want %#v", got, aggregateRows)
	}
	if got := recoveryLease.JoinArrangements()[0].Rows(); !reflect.DeepEqual(got, joinRows) {
		t.Fatalf("recovered join rows = %#v, want %#v", got, joinRows)
	}
	if recoveredAggregates.Active() != 1 || recoveredJoins.Active() != 1 {
		t.Fatalf("recovered active arrangements = %d aggregate, %d join", recoveredAggregates.Active(), recoveredJoins.Active())
	}
	if !recoveryLease.Release() {
		t.Fatal("first recovery lease release = false")
	}
	if recoveryLease.Release() {
		t.Fatal("second recovery lease release = true")
	}
	if recoveredAggregates.Active() != 0 || recoveredJoins.Active() != 0 {
		t.Fatalf("released active arrangements = %d aggregate, %d join", recoveredAggregates.Active(), recoveredJoins.Active())
	}
}

func TestTypedTableArrangementRecoveryRollsBackAcrossCatalogs(t *testing.T) {
	aggregateTable, aggregateChanges, aggregateDefinition := mU05AggregateTable(t)
	aggregates, err := hatSql.NewTypedTableAggregateArrangements(aggregateTable)
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := aggregates.Acquire(aggregateDefinition)
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply(aggregateChanges); err != nil {
		t.Fatal(err)
	}

	left, leftChanges := mU05JoinTable(t, "left", []string{"left-red"})
	right, rightChanges := mU05JoinTable(t, "right", []string{"right-red"})
	joins, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	join, err := joins.Acquire(hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyLeft(leftChanges); err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyRight(rightChanges); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := hatSql.CaptureTypedTableArrangementRecovery(
		[]*hatSql.TypedTableAggregateArrangements{aggregates},
		[]*hatSql.TypedTableJoinArrangements{joins},
	)
	if err != nil {
		t.Fatal(err)
	}
	aggregate.Release()
	join.Release()

	bad := checkpoint
	bad.Joins = append([]hatSql.TypedTableJoinArrangementCheckpoint(nil), checkpoint.Joins...)
	bad.Joins[0].RightSourceSequence++
	recoveredAggregates, err := hatSql.NewTypedTableAggregateArrangements(aggregateTable)
	if err != nil {
		t.Fatal(err)
	}
	recoveredJoins, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if lease, err := hatSql.RestoreTypedTableArrangementRecovery(
		[]*hatSql.TypedTableAggregateArrangements{recoveredAggregates},
		[]*hatSql.TypedTableJoinArrangements{recoveredJoins},
		bad,
	); lease != nil || !errors.Is(err, hatSql.ErrTypedTableArrangementSourceVersionMismatch) {
		t.Fatalf("cross-catalog restore lease = %#v, error = %v", lease, err)
	}
	if recoveredAggregates.Active() != 0 || recoveredJoins.Active() != 0 {
		t.Fatalf("failed recovery left %d aggregate and %d join arrangements", recoveredAggregates.Active(), recoveredJoins.Active())
	}
}

func TestTypedTableArrangementRecoveryValidatesTargetsBeforeMutation(t *testing.T) {
	table, changes, definition := mU05AggregateTable(t)
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Apply(changes); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := hatSql.CaptureTypedTableArrangementRecovery(
		[]*hatSql.TypedTableAggregateArrangements{arrangements},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	arrangement.Release()

	duplicate := checkpoint
	duplicate.Aggregates = append([]hatSql.TypedTableAggregateArrangementCheckpoint(nil), checkpoint.Aggregates...)
	duplicate.Aggregates = append(duplicate.Aggregates, duplicate.Aggregates[0])
	recovered, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	if lease, err := hatSql.RestoreTypedTableArrangementRecovery(
		[]*hatSql.TypedTableAggregateArrangements{recovered},
		nil,
		duplicate,
	); lease != nil || !errors.Is(err, hatSql.ErrTypedTableArrangementCheckpointDuplicate) {
		t.Fatalf("duplicate recovery lease = %#v, error = %v", lease, err)
	}
	if recovered.Active() != 0 {
		t.Fatalf("duplicate recovery left %d active arrangements", recovered.Active())
	}

	missing, err := hatSql.RestoreTypedTableArrangementRecovery(nil, nil, checkpoint)
	if missing != nil || !errors.Is(err, hatSql.ErrTypedTableArrangementRecoveryCatalogNotFound) {
		t.Fatalf("missing recovery lease = %#v, error = %v", missing, err)
	}

	activeTarget, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	live, err := activeTarget.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	if lease, err := hatSql.RestoreTypedTableArrangementRecovery(
		[]*hatSql.TypedTableAggregateArrangements{activeTarget},
		nil,
		checkpoint,
	); lease != nil || !errors.Is(err, hatSql.ErrTypedTableArrangementRecoveryTargetActive) {
		t.Fatalf("active target recovery lease = %#v, error = %v", lease, err)
	}
	if activeTarget.Active() != 1 {
		t.Fatalf("active target changed to %d arrangements", activeTarget.Active())
	}
	live.Release()
}
