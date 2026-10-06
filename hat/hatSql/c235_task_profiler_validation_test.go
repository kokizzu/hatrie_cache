package hatSql

import (
	"errors"
	"testing"
)

func TestC235TaskProfilerRejectsMalformedIdentities(t *testing.T) {
	profiler, err := NewSQLTaskProfiler(SQLTaskProfilerOptions{})
	if err != nil {
		t.Fatalf("NewSQLTaskProfiler() error = %v", err)
	}

	tests := []struct {
		name   string
		sample SQLTaskProfileSample
	}{
		{
			name: "blank table",
			sample: SQLTaskProfileSample{
				Operation: SQLTaskRead,
				Table:     " \t",
				Part:      "part-01",
			},
		},
		{
			name: "invalid part utf8",
			sample: SQLTaskProfileSample{
				Operation: SQLTaskRead,
				Table:     "orders",
				Part:      string([]byte{0xff}),
			},
		},
		{
			name: "invalid column utf8",
			sample: SQLTaskProfileSample{
				Operation: SQLTaskRead,
				Table:     "orders",
				Part:      "part-01",
				Column:    string([]byte{0xff}),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := profiler.Record(test.sample); !errors.Is(err, ErrSQLTaskProfilerInputInvalid) {
				t.Fatalf("Record() error = %v, want %v", err, ErrSQLTaskProfilerInputInvalid)
			}
		})
	}
}
