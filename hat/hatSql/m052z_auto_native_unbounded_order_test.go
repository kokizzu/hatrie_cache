package hatSql

import (
	"context"
	"reflect"
	"testing"
)

type m052zUnboundedOrderResolver struct {
	rows []SQLRow
}

func (resolver m052zUnboundedOrderResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func m052zUnboundedOrderRows() []SQLRow {
	rows := make([]SQLRow, 4096)
	for index := range rows {
		rows[index] = SQLRow{
			"id":    int64(index),
			"value": int64((index * 7919) % len(rows)),
		}
	}
	return rows
}

func m052zUnboundedOrderQuery() string {
	return "FROM CACHE('items') AS item SELECT item.id, item.value WHERE item.value >= 0 ORDER BY item.value DESC"
}

func TestM052zAutomaticNativeUnboundedOrder(t *testing.T) {
	resolver := m052zUnboundedOrderResolver{rows: m052zUnboundedOrderRows()}
	for _, source := range []string{
		m052zUnboundedOrderQuery(),
		m052zUnboundedOrderQuery() + " OFFSET 512",
	} {
		query, err := parseSQLQueryWithCache(source, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		detail, eligible := sqlAutoNativeDataflowPlanDetail(query, resolver, SQLQueryOptions{})
		if !eligible || detail != "automatic full-order batch execution" {
			t.Fatalf("automatic plan for %q = %q/%t, want full-order native execution", source, detail, eligible)
		}

		got, err := ExecuteSQLQueryContext(context.Background(), source, resolver, SQLQueryOptions{
			Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
		})
		if err != nil {
			t.Fatal(err)
		}
		if !m052qPlanHasNode(got.Plan, "NATIVE DATAFLOW") {
			t.Fatalf("automatic plan for %q = %#v, want NATIVE DATAFLOW", source, got.Plan)
		}
		want, err := ExecuteSQLQueryContext(context.Background(), source, resolver, SQLQueryOptions{DisableNativeDataflow: true})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Columns, want.Columns) || !reflect.DeepEqual(got.Rows, want.Rows) {
			t.Fatalf("native rows differ from fallback for %q: native=%#v fallback=%#v", source, got, want)
		}
		for index := 1; index < len(got.Rows); index++ {
			if got.Rows[index-1]["value"].(int64) < got.Rows[index]["value"].(int64) {
				t.Fatalf("rows are not descending for %q at %d: %#v then %#v", source, index, got.Rows[index-1], got.Rows[index])
			}
		}
	}
}

var m052zUnboundedOrderSink SQLQueryResult

func BenchmarkM052zUnboundedOrder(b *testing.B) {
	resolver := m052zUnboundedOrderResolver{rows: m052zUnboundedOrderRows()}
	query := m052zUnboundedOrderQuery()
	for _, benchmark := range []struct {
		name    string
		options SQLQueryOptions
	}{
		{name: "fallback", options: SQLQueryOptions{DisableNativeDataflow: true}},
		{name: "automatic", options: SQLQueryOptions{}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, benchmark.options)
				if err != nil {
					b.Fatal(err)
				}
				m052zUnboundedOrderSink = result
			}
		})
	}
}
