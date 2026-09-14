package hatSql_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type ch029LookupJoinResolver struct {
	orders      []hatSql.Row
	lookupCalls int
	scanCalls   int
}

func (resolver *ch029LookupJoinResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name == "CACHE" && key == "orders" {
		return resolver.orders, nil
	}
	return nil, nil
}

func (resolver *ch029LookupJoinResolver) ResolveSQLLookupSource(name, key, field string, value interface{}) ([]hatSql.Row, bool, error) {
	resolver.lookupCalls++
	if name != "EXTERNAL" || key != "countries" || field != "code" {
		return nil, false, nil
	}
	if value == nil {
		return nil, true, nil
	}
	code, ok := value.(string)
	if !ok {
		return nil, true, nil
	}
	switch code {
	case "SG":
		return []hatSql.Row{{"code": "SG", "name": "Singapore"}}, true, nil
	case "JP":
		return []hatSql.Row{{"code": "JP", "name": "Japan"}}, true, nil
	default:
		return nil, true, nil
	}
}

func (resolver *ch029LookupJoinResolver) ResolveSQLExternalSource(string) ([]hatSql.Row, error) {
	resolver.scanCalls++
	return nil, nil
}

func TestCH029DictionaryLookupJoinUsesPointLookups(t *testing.T) {
	resolver := &ch029LookupJoinResolver{orders: []hatSql.Row{
		{"id": int64(1), "country": "SG"},
		{"id": int64(2), "country": "JP"},
		{"id": int64(3), "country": "XX"},
	}}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM CACHE('orders') AS order_row
JOIN EXTERNAL('countries') AS country ON order_row.country = country.code
SELECT order_row.id, country.name
ORDER BY order_row.id`, resolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{
		{"id": int64(1), "name": "Singapore"},
		{"id": int64(2), "name": "Japan"},
	}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("inner join rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.lookupCalls != 4 {
		t.Fatalf("lookup calls = %d, want availability probe plus one per order", resolver.lookupCalls)
	}
	if resolver.scanCalls != 0 {
		t.Fatalf("external scan calls = %d, want zero", resolver.scanCalls)
	}
}

func TestCH029DictionaryLookupLeftJoinPreservesMissingRows(t *testing.T) {
	resolver := &ch029LookupJoinResolver{orders: []hatSql.Row{
		{"id": int64(1), "country": "XX"},
	}}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM CACHE('orders') AS order_row
LEFT JOIN EXTERNAL('countries') AS country ON order_row.country = country.code
SELECT order_row.id, country.name`, resolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": int64(1), "name": nil}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("left join rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.scanCalls != 0 {
		t.Fatalf("external scan calls = %d, want zero", resolver.scanCalls)
	}
}
