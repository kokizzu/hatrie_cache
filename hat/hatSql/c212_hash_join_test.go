package hatSql

import (
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestC212PrecomputedJoinIndexPreservesKeySemantics(t *testing.T) {
	index := newSQLJoinHashIndex(8)
	values := []interface{}{
		int64(7),
		float64(7),
		"7",
		true,
		false,
		math.Copysign(0, -1),
		float64(0),
		math.NaN(),
	}
	for row, value := range values {
		if !index.Add(value, row) {
			t.Fatalf("Add(%#v) rejected a supported key", value)
		}
	}

	assertC212JoinIndexRows(t, index.Lookup(int(7)), []int{0, 1})
	assertC212JoinIndexRows(t, index.Lookup("7"), []int{2})
	assertC212JoinIndexRows(t, index.Lookup(true), []int{3})
	assertC212JoinIndexRows(t, index.Lookup(false), []int{4})
	assertC212JoinIndexRows(t, index.Lookup(math.Copysign(0, -1)), []int{5})
	assertC212JoinIndexRows(t, index.Lookup(float64(0)), []int{6})
	assertC212JoinIndexRows(t, index.Lookup(math.NaN()), []int{7})
	if got := index.Lookup(int64(99)); got != nil {
		t.Fatalf("missing key lookup = %v, want nil", got)
	}
}

func TestC212PrecomputedJoinIndexDistinguishesDistinctStringKeys(t *testing.T) {
	index := newSQLJoinHashIndex(2)
	first := sqlJoinProbeKey{kind: sqlJoinProbeString, stringValue: "first"}
	second := sqlJoinProbeKey{kind: sqlJoinProbeString, stringValue: "second"}
	index.addKey(first, 10)
	index.addKey(second, 20)
	index.addKey(first, 30)

	assertC212JoinIndexRows(t, index.lookupKey(first), []int{10, 30})
	assertC212JoinIndexRows(t, index.lookupKey(second), []int{20})
}

func TestC212PrecomputedJoinKeyMatchesCanonicalJoinKeySemantics(t *testing.T) {
	values := []interface{}{
		nil,
		int(7),
		int64(7),
		float32(7),
		float64(7),
		math.Copysign(0, -1),
		float64(0),
		math.NaN(),
		"7",
		true,
		false,
		[]byte("7"),
		struct{}{},
	}
	for leftIndex, left := range values {
		for rightIndex, right := range values {
			leftCanonical, leftCanonicalOK := sqlHashJoinKey(left)
			rightCanonical, rightCanonicalOK := sqlHashJoinKey(right)
			leftProbe, leftProbeOK := newSQLJoinProbeKey(left)
			rightProbe, rightProbeOK := newSQLJoinProbeKey(right)
			if leftCanonicalOK != leftProbeOK || rightCanonicalOK != rightProbeOK {
				t.Fatalf("support mismatch for values[%d]=%#v and values[%d]=%#v", leftIndex, left, rightIndex, right)
			}
			canonicalEqual := leftCanonicalOK && rightCanonicalOK && leftCanonical == rightCanonical
			probeEqual := leftProbeOK && rightProbeOK && leftProbe.equal(rightProbe)
			if canonicalEqual != probeEqual {
				t.Fatalf("equality mismatch for values[%d]=%#v and values[%d]=%#v", leftIndex, left, rightIndex, right)
			}
		}
	}
}

type c212TypedJoinResolver struct {
	sources map[string][]Row
}

func (resolver c212TypedJoinResolver) ResolveSQLSource(_, key string) ([]Row, error) {
	return resolver.sources[key], nil
}

func TestC212SQLJoinSupportsNativeStringAndBooleanProbeKeys(t *testing.T) {
	query := "FROM CACHE('left') AS l INNER JOIN CACHE('middle') AS m ON l.key = m.key INNER JOIN CACHE('right') AS r ON m.key = r.key SELECT l.key"
	resolver := c212TypedJoinResolver{sources: map[string][]Row{
		"left":   {{"key": "a"}, {"key": true}, {"key": "b"}, {"key": false}},
		"middle": {{"key": true}, {"key": "b"}, {"key": false}, {"key": "a"}},
		"right":  {{"key": "b"}, {"key": false}, {"key": "a"}, {"key": true}},
	}}
	result, err := ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]interface{}, len(result.Rows))
	for index, row := range result.Rows {
		got[index] = row["key"]
	}
	want := []interface{}{"a", true, "b", false}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("joined keys = %#v, want %#v", got, want)
	}
}

func assertC212JoinIndexRows(t *testing.T, got, want []int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("rows = %v, want %v", got, want)
		}
	}
}

var c212HashJoinBenchmarkSink int

func BenchmarkC212JoinIndexNumeric(b *testing.B) {
	values := make([]interface{}, 256)
	for index := range values {
		values[index] = int64(index)
	}
	benchmarkC212JoinIndex(b, values)
}

func BenchmarkC212CanonicalMapNumeric(b *testing.B) {
	values := make([]interface{}, 256)
	for index := range values {
		values[index] = int64(index)
	}
	benchmarkC212CanonicalMap(b, values)
}

func BenchmarkC212JoinIndexString(b *testing.B) {
	values := make([]interface{}, 256)
	for index := range values {
		values[index] = "key-" + strconv.Itoa(index)
	}
	benchmarkC212JoinIndex(b, values)
}

func BenchmarkC212CanonicalMapString(b *testing.B) {
	values := make([]interface{}, 256)
	for index := range values {
		values[index] = "key-" + strconv.Itoa(index)
	}
	benchmarkC212CanonicalMap(b, values)
}

func benchmarkC212JoinIndex(b *testing.B, values []interface{}) {
	b.ReportAllocs()
	for range b.N {
		index := newSQLJoinHashIndex(len(values))
		for row, value := range values {
			index.Add(value, row)
		}
		for _, value := range values {
			c212HashJoinBenchmarkSink += len(index.Lookup(value))
		}
	}
}

func benchmarkC212CanonicalMap(b *testing.B, values []interface{}) {
	b.ReportAllocs()
	for range b.N {
		index := make(map[string][]int, len(values))
		for row, value := range values {
			key, ok := sqlHashJoinKey(value)
			if ok {
				index[key] = append(index[key], row)
			}
		}
		for _, value := range values {
			key, ok := sqlHashJoinKey(value)
			if ok {
				c212HashJoinBenchmarkSink += len(index[key])
			}
		}
	}
}
