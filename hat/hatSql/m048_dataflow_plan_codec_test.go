package hatSql

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestM048DataflowPlanBinaryRoundTripAndJSONFallback(t *testing.T) {
	want := m048DataflowPlanBenchmarkPlan()
	encoded, err := EncodeSQLDataflowPlan(want)
	if err != nil {
		t.Fatalf("EncodeSQLDataflowPlan() error = %v", err)
	}
	if !bytes.HasPrefix(encoded, []byte("HDP1")) {
		t.Fatalf("binary prefix = %q, want HDP1", encoded[:minM048DataflowPlanBytes(len(encoded), 4)])
	}
	got, err := DecodeSQLDataflowPlan(encoded)
	if err != nil {
		t.Fatalf("DecodeSQLDataflowPlan() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("binary round trip = %#v, want %#v", got, want)
	}

	jsonEncoded, err := EncodeSQLDataflowPlanJSON(want)
	if err != nil {
		t.Fatalf("EncodeSQLDataflowPlanJSON() error = %v", err)
	}
	jsonGot, err := DecodeSQLDataflowPlan(jsonEncoded)
	if err != nil {
		t.Fatalf("DecodeSQLDataflowPlan(JSON) error = %v", err)
	}
	if !reflect.DeepEqual(jsonGot, want) {
		t.Fatalf("JSON fallback round trip = %#v, want %#v", jsonGot, want)
	}
}

func TestM048DataflowPlanBinaryRejectsMalformedAndInvalidPlans(t *testing.T) {
	valid, err := EncodeSQLDataflowPlan(m048DataflowPlanBenchmarkPlan())
	if err != nil {
		t.Fatalf("EncodeSQLDataflowPlan() error = %v", err)
	}
	for _, testCase := range []struct {
		name string
		data []byte
		want error
	}{
		{name: "empty", data: nil, want: ErrSQLDataflowPlanBinaryInvalid},
		{name: "bad magic", data: []byte("nope"), want: ErrSQLDataflowPlanBinaryInvalid},
		{name: "trailing bytes", data: append(append([]byte(nil), valid...), 1), want: ErrSQLDataflowPlanBinaryInvalid},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := DecodeSQLDataflowPlan(testCase.data); !errors.Is(err, testCase.want) {
				t.Fatalf("DecodeSQLDataflowPlan() error = %v, want %v", err, testCase.want)
			}
		})
	}
	invalid := m048DataflowPlanBenchmarkPlan()
	invalid.Format = "wrong"
	if _, err := EncodeSQLDataflowPlan(invalid); !errors.Is(err, ErrSQLDataflowPlanInvalid) {
		t.Fatalf("invalid format error = %v, want %v", err, ErrSQLDataflowPlanInvalid)
	}
}

func TestM048DataflowPlanBinaryRejectsEveryTruncatedPrefixAndPreservesReceiver(t *testing.T) {
	valid, err := EncodeSQLDataflowPlan(m048DataflowPlanBenchmarkPlan())
	if err != nil {
		t.Fatalf("EncodeSQLDataflowPlan() error = %v", err)
	}
	for length := 0; length < len(valid); length++ {
		if _, err := DecodeSQLDataflowPlan(valid[:length]); !errors.Is(err, ErrSQLDataflowPlanBinaryInvalid) {
			t.Fatalf("prefix length %d error = %v, want ErrSQLDataflowPlanBinaryInvalid", length, err)
		}
	}
	original := SQLDataflowPlan{Format: sqlDataflowPlanFormat, Source: "keep", Root: -1}
	receiver := original
	if err := receiver.UnmarshalBinary(append(append([]byte(nil), valid...), 0)); !errors.Is(err, ErrSQLDataflowPlanBinaryInvalid) {
		t.Fatalf("trailing byte UnmarshalBinary() error = %v, want ErrSQLDataflowPlanBinaryInvalid", err)
	}
	if !reflect.DeepEqual(receiver, original) {
		t.Fatalf("receiver after failed decode = %#v, want %#v", receiver, original)
	}
}

func TestM048DataflowPlanJSONRejectsInvalidPlan(t *testing.T) {
	if _, err := DecodeSQLDataflowPlan([]byte(`{"format":"wrong","root":-1}`)); !errors.Is(err, ErrSQLDataflowPlanJSONInvalid) {
		t.Fatalf("invalid JSON plan error = %v, want ErrSQLDataflowPlanJSONInvalid", err)
	}
}

func minM048DataflowPlanBytes(left, right int) int {
	if left < right {
		return left
	}
	return right
}
