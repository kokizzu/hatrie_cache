package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

type mu039RowsResolver struct{}

func (mu039RowsResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	return []hatSql.Row{
		{"id": int64(1), "region": "apac", "created_at": int64(20)},
		{"id": int64(2), "region": "us", "created_at": int64(10)},
	}, nil
}

func mu039Declaration() hatSql.SQLPartitionDeclaration {
	return hatSql.SQLPartitionDeclaration{
		Namespace:      "default",
		Source:         "orders",
		Kind:           "CACHE",
		PartitionBy:    []string{"region"},
		OrderBy:        []hatSql.SQLPartitionOrder{{Field: "created_at", Desc: true}},
		PartitionCount: 3,
	}
}

func TestMU039PartitionDeclarationCatalogAndExplain(t *testing.T) {
	resolver := hatSql.CatalogResolver{
		Source: mu039RowsResolver{},
		Catalog: hatSql.Catalog{
			Partitions: []hatSql.SQLPartitionDeclaration{mu039Declaration()},
		},
	}

	declaration, available, err := resolver.ResolveSQLPartitionDeclaration("CACHE", "orders")
	if err != nil {
		t.Fatalf("ResolveSQLPartitionDeclaration() error = %v", err)
	}
	if !available || declaration.PartitionCount != 3 {
		t.Fatalf("ResolveSQLPartitionDeclaration() = %#v, available=%v", declaration, available)
	}

	catalogRows, err := resolver.ResolveSQLSource("CACHE", "information_schema.partitions")
	if err != nil {
		t.Fatalf("ResolveSQLSource(partitions) error = %v", err)
	}
	if len(catalogRows) != 2 {
		t.Fatalf("partition catalog rows = %d, want 2", len(catalogRows))
	}
	if catalogRows[0]["role"] != "PARTITION" || catalogRows[0]["field"] != "region" {
		t.Fatalf("partition catalog row = %#v", catalogRows[0])
	}
	if catalogRows[1]["role"] != "ORDER" || catalogRows[1]["field"] != "created_at" || catalogRows[1]["descending"] != true {
		t.Fatalf("order catalog row = %#v", catalogRows[1])
	}
	showResult, err := hatSql.ExecuteSQLQuery("SHOW PARTITIONS", resolver)
	if err != nil {
		t.Fatalf("SHOW PARTITIONS error = %v", err)
	}
	if len(showResult.Rows) != 2 || showResult.Rows[0]["source"] != "orders" {
		t.Fatalf("SHOW PARTITIONS rows = %#v", showResult.Rows)
	}

	result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('orders') SELECT id ORDER BY created_at DESC", resolver)
	if err != nil {
		t.Fatalf("EXPLAIN error = %v", err)
	}
	if len(result.Partitioning) != 1 {
		t.Fatalf("EXPLAIN partitioning annotations = %#v", result.Partitioning)
	}
	annotation := result.Partitioning[0]
	if annotation.StepIndex < 0 || annotation.StepIndex >= len(result.Plan) || result.Plan[annotation.StepIndex].Node != "SCAN" {
		t.Fatalf("EXPLAIN partition annotation step = %#v", annotation)
	}
	if len(annotation.Declaration.PartitionBy) != 1 || annotation.Declaration.PartitionBy[0] != "region" {
		t.Fatalf("EXPLAIN partition key = %#v", annotation.Declaration)
	}
	if len(annotation.Declaration.OrderBy) != 1 || annotation.Declaration.OrderBy[0].Field != "created_at" || !annotation.Declaration.OrderBy[0].Desc {
		t.Fatalf("EXPLAIN order = %#v", annotation.Declaration)
	}
	queryResult, err := hatSql.ExecuteSQLQuery("FROM CACHE('orders') WHERE region = 'us' SELECT id", resolver)
	if err != nil {
		t.Fatalf("ordinary query error = %v", err)
	}
	if len(queryResult.Rows) != 1 || queryResult.Rows[0]["id"] != int64(2) {
		t.Fatalf("ordinary query rows = %#v", queryResult.Rows)
	}
}

func TestMU039PartitionDeclarationForwardsThroughSession(t *testing.T) {
	declaration := mu039Declaration()
	session := hatSql.NewSQLSession(hatSql.CatalogResolver{
		Source:  mu039RowsResolver{},
		Catalog: hatSql.Catalog{Partitions: []hatSql.SQLPartitionDeclaration{declaration}},
	})
	got, available, err := session.ResolveSQLPartitionDeclaration("CACHE", "orders")
	if err != nil {
		t.Fatalf("session ResolveSQLPartitionDeclaration() error = %v", err)
	}
	if !available || got.Source != declaration.Source || got.PartitionCount != declaration.PartitionCount {
		t.Fatalf("session declaration = %#v, available=%v", got, available)
	}
}

func TestMU039PartitionDeclarationRejectsInvalidMetadata(t *testing.T) {
	if err := hatSql.ValidateSQLPartitionDeclaration(hatSql.SQLPartitionDeclaration{
		Source:      "orders",
		PartitionBy: []string{"region"},
		OrderBy:     []hatSql.SQLPartitionOrder{{Field: "region"}},
	}); err != nil {
		t.Fatalf("same field in partition and order lists rejected: %v", err)
	}
	resolver := hatSql.CatalogResolver{Catalog: hatSql.Catalog{Partitions: []hatSql.SQLPartitionDeclaration{{Source: "orders", PartitionBy: []string{""}}}}}
	if _, _, err := resolver.ResolveSQLPartitionDeclaration("CACHE", "orders"); err == nil {
		t.Fatal("invalid partition declaration unexpectedly accepted")
	}
}
