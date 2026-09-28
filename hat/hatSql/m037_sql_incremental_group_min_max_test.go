package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

const m037SQLGroupMinMaxQuery = "FROM CACHE('items') AS src SELECT src.group, MIN(src.value) AS lowest, MAX(src.value) AS highest GROUP BY src.group"

func TestM037SQLIncrementalGroupMinMaxMaintainsEndpoints(t *testing.T) {
	compiled, err := CompileSQLQuery(m037SQLGroupMinMaxQuery)
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	operator, err := compiled.CompileIncrementalGroupAggregate()
	if err != nil {
		t.Fatalf("CompileIncrementalGroupAggregate() error = %v", err)
	}
	seed := []DifferentialRow{
		{Key: "red-1", Time: 1, Diff: 1, Row: Row{"group": "red", "value": int64(5)}},
		{Key: "red-2", Time: 1, Diff: 1, Row: Row{"group": "red", "value": int64(5)}},
		{Key: "red-3", Time: 1, Diff: 1, Row: Row{"group": "red", "value": int64(7)}},
		{Key: "blue-1", Time: 1, Diff: 1, Row: Row{"group": "blue", "value": int64(9)}},
	}
	if _, err := operator.Apply(seed); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: `"blue"`, Diff: 1, Row: Row{"group": "blue", "lowest": int64(9), "highest": int64(9)}},
		{Key: `"red"`, Diff: 1, Row: Row{"group": "red", "lowest": int64(5), "highest": int64(7)}},
	}
	if got := operator.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, want)
	}

	if got, err := operator.Apply([]DifferentialRow{{Key: "red-1", Time: 2, Diff: -1, Row: Row{"group": "red", "value": int64(5)}}}); err != nil || got != nil {
		t.Fatalf("duplicate endpoint retraction = %#v, error %v, want nil output and nil error", got, err)
	}
	got, err := operator.Apply([]DifferentialRow{{Key: "red-2", Time: 3, Diff: -1, Row: Row{"group": "red", "value": int64(5)}}})
	if err != nil {
		t.Fatalf("endpoint retraction error = %v", err)
	}
	wantChanges := []DifferentialRow{
		{Key: `"red"`, Time: 3, Diff: -1, Row: Row{"group": "red", "lowest": int64(5), "highest": int64(7)}},
		{Key: `"red"`, Time: 3, Diff: 1, Row: Row{"group": "red", "lowest": int64(7), "highest": int64(7)}},
	}
	if !reflect.DeepEqual(got, wantChanges) {
		t.Fatalf("endpoint retraction = %#v, want %#v", got, wantChanges)
	}

	got, err = operator.Apply([]DifferentialRow{{Key: "red-3", Time: 4, Diff: -1, Row: Row{"group": "red", "value": int64(7)}}})
	if err != nil {
		t.Fatalf("group removal error = %v", err)
	}
	want = []DifferentialRow{
		{Key: `"red"`, Time: 4, Diff: -1, Row: Row{"group": "red", "lowest": int64(7), "highest": int64(7)}},
		{Key: `"blue"`, Diff: 1, Row: Row{"group": "blue", "lowest": int64(9), "highest": int64(9)}},
	}
	if gotSnapshot := operator.Snapshot(); !reflect.DeepEqual(gotSnapshot, want[1:]) {
		t.Fatalf("Snapshot() after group removal = %#v, want %#v", gotSnapshot, want[1:])
	}
	if !reflect.DeepEqual(got, want[:1]) {
		t.Fatalf("group removal changes = %#v, want %#v", got, want[:1])
	}
}

func TestM037SQLIncrementalGroupMinMaxWhereAndAtomicity(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.group, MIN(src.value) AS lowest, MAX(src.value) AS highest WHERE src.active GROUP BY src.group")
	if err != nil {
		t.Fatalf("CompileSQLQuery() error = %v", err)
	}
	operator, err := compiled.CompileIncrementalGroupAggregate()
	if err != nil {
		t.Fatalf("CompileIncrementalGroupAggregate() error = %v", err)
	}
	if _, err := operator.Apply([]DifferentialRow{
		{Key: "active", Diff: 1, Row: Row{"group": "red", "value": int64(3), "active": true}},
		{Key: "inactive", Diff: 1, Row: Row{"group": "red", "value": int64(1), "active": false}},
	}); err != nil {
		t.Fatalf("WHERE Apply() error = %v", err)
	}
	want := []DifferentialRow{{Key: `"red"`, Diff: 1, Row: Row{"group": "red", "lowest": int64(3), "highest": int64(3)}}}
	if got := operator.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("WHERE Snapshot() = %#v, want %#v", got, want)
	}

	_, err = operator.Apply([]DifferentialRow{
		{Key: "blue", Diff: 1, Row: Row{"group": "blue", "value": int64(8), "active": true}},
		{Key: "bad", Diff: 1, Row: Row{"group": "red", "value": "not-an-int", "active": true}},
	})
	if !errors.Is(err, ErrSQLIncrementalGroupAggregateValueType) {
		t.Fatalf("invalid value error = %v, want ErrSQLIncrementalGroupAggregateValueType", err)
	}
	if got := operator.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() after rejected batch = %#v, want %#v", got, want)
	}
}

func TestM037SQLIncrementalGroupMinMaxMatchesMaterialized(t *testing.T) {
	rows := []SQLRow{
		{"group": "red", "value": int64(5)},
		{"group": "red", "value": int64(7)},
		{"group": "blue", "value": int64(9)},
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	result, err := ExecuteSQLQuery(m037SQLGroupMinMaxQuery, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	got := make(map[string]SQLRow, len(result.Rows))
	for _, row := range result.Rows {
		lowest, ok := m037SQLInt64(row["lowest"])
		if !ok {
			t.Fatalf("materialized lowest type = %T, want integer", row["lowest"])
		}
		highest, ok := m037SQLInt64(row["highest"])
		if !ok {
			t.Fatalf("materialized highest type = %T, want integer", row["highest"])
		}
		got[row["group"].(string)] = SQLRow{"group": row["group"], "lowest": lowest, "highest": highest}
	}
	want := map[string]SQLRow{
		"blue": {"group": "blue", "lowest": int64(9), "highest": int64(9)},
		"red":  {"group": "red", "lowest": int64(5), "highest": int64(7)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("materialized result = %#v, want %#v", got, want)
	}
}

func m037SQLInt64(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		return int64(value), uint64(value) <= uint64(^uint64(0)>>1)
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		return int64(value), value <= uint64(^uint64(0)>>1)
	case float64:
		return int64(value), float64(int64(value)) == value
	default:
		return 0, false
	}
}

func TestM037SQLIncrementalGroupMinMaxRejectsUnsupportedShapes(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.group, MIN(src.value) AS lowest, COUNT(*) AS rows GROUP BY src.group",
		"FROM CACHE('items') AS src SELECT src.group, MIN(src.value) AS lowest GROUP BY src.group ORDER BY src.group",
		"FROM CACHE('items') AS src SELECT src.group, MIN(src.value) AS lowest, MIN(src.value) AS other GROUP BY src.group",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			compiled, err := CompileSQLQuery(query)
			if err != nil {
				return
			}
			if _, err := compiled.CompileIncrementalGroupAggregate(); !errors.Is(err, ErrSQLIncrementalGroupAggregateUnsupported) {
				t.Fatalf("CompileIncrementalGroupAggregate() error = %v, want unsupported", err)
			}
		})
	}
}
