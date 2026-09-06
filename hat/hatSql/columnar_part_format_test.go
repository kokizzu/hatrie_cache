package hatSql

import "testing"

func TestSelectSQLColumnarPartFormatUsesRowAndWidthThresholds(t *testing.T) {
	policy := DefaultSQLColumnarPartFormatPolicy()
	for name, test := range map[string]struct {
		rows  int64
		bytes int64
		want  SQLColumnarPartFormat
	}{
		"below thresholds": {rows: policy.MaxCompactRows - 1, bytes: policy.MaxCompactBytes - 1, want: SQLColumnarPartCompact},
		"row boundary":     {rows: policy.MaxCompactRows, bytes: policy.MaxCompactBytes, want: SQLColumnarPartCompact},
		"too many rows":    {rows: policy.MaxCompactRows + 1, bytes: policy.MaxCompactBytes, want: SQLColumnarPartWide},
		"too wide":         {rows: policy.MaxCompactRows, bytes: policy.MaxCompactBytes + 1, want: SQLColumnarPartWide},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := SelectSQLColumnarPartFormat(test.rows, test.bytes, policy)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("format = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSelectSQLColumnarPartFormatSupportsCustomPolicy(t *testing.T) {
	policy := SQLColumnarPartFormatPolicy{MaxCompactRows: 10, MaxCompactBytes: 100}
	got, err := SelectSQLColumnarPartFormat(10, 100, policy)
	if err != nil {
		t.Fatal(err)
	}
	if got != SQLColumnarPartCompact {
		t.Fatalf("custom boundary format = %q, want %q", got, SQLColumnarPartCompact)
	}
}

func TestSelectSQLColumnarPartFormatRejectsInvalidInputs(t *testing.T) {
	policy := DefaultSQLColumnarPartFormatPolicy()
	for name, values := range map[string][2]int64{
		"negative rows":  {-1, 1},
		"negative bytes": {1, -1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := SelectSQLColumnarPartFormat(values[0], values[1], policy); err == nil {
				t.Fatal("SelectSQLColumnarPartFormat() error = nil")
			}
		})
	}
	for name, policy := range map[string]SQLColumnarPartFormatPolicy{
		"zero rows":      {MaxCompactRows: 0, MaxCompactBytes: 1},
		"negative rows":  {MaxCompactRows: -1, MaxCompactBytes: 1},
		"zero bytes":     {MaxCompactRows: 1, MaxCompactBytes: 0},
		"negative bytes": {MaxCompactRows: 1, MaxCompactBytes: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := SelectSQLColumnarPartFormat(1, 1, policy); err == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}
}

func BenchmarkSelectSQLColumnarPartFormat(b *testing.B) {
	policy := DefaultSQLColumnarPartFormatPolicy()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := SelectSQLColumnarPartFormat(policy.MaxCompactRows, policy.MaxCompactBytes, policy); err != nil {
			b.Fatal(err)
		}
	}
}
