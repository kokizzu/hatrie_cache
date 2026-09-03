package hatCache

import (
	"context"
	"testing"
)

func TestExplainSQLWhatIfFacade(t *testing.T) {
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{{"id": int64(1), "region": "apac"}, {"id": int64(2), "region": "emea"}}, nil
	})
	report, err := ExplainSQLWhatIf(context.Background(), SQLWhatIfRequest{
		Query: "SELECT id FROM CACHE('events') WHERE region = 'apac'",
		Index: SQLWhatIfIndex{Kind: SQLWhatIfIndexEquality, Fields: []string{"region"}},
	}, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Supported || report.HypotheticalRowsRead != 1 {
		t.Fatalf("facade report = %#v", report)
	}
}
