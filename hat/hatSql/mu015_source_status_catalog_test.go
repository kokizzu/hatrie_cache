package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestMU015CatalogResolverExposesStableSourceStatusSnapshot(t *testing.T) {
	calls := 0
	resolver := CatalogResolver{
		Catalog: Catalog{
			Version: 17,
			Sources: []CatalogSource{
				{Namespace: "public", Name: "orders", Kind: "CACHE"},
				{Namespace: "public", Name: "people", Kind: "CACHE"},
			},
		},
		SourceStatus: CatalogSourceStatusResolverFunc(func() ([]CatalogSourceStatus, error) {
			calls++
			return []CatalogSourceStatus{{
				Namespace: "public",
				Source:    "orders",
				Kind:      "CACHE",
				State:     CatalogSourceStateRunning,
				Available: true,
				Frontier:  8,
				Observed:  10,
				ErrorCode: "timeout",
			}}, nil
		}),
	}

	result, err := ExecuteSQLQueryParameters(context.Background(), `FROM CACHE('information_schema.source_status') SELECT catalog_version, source, kind, state, available, ready, frontier, observed, lag, error_code ORDER BY source`, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("source status query error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("source status resolver calls = %d, want one snapshot call", calls)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("source status rows = %#v, want known and unknown source", result.Rows)
	}
	orders := result.Rows[0]
	if orders["source"] != "orders" || orders["catalog_version"] != uint64(17) || orders["kind"] != "CACHE" || orders["state"] != CatalogSourceStateRunning || orders["available"] != true || orders["ready"] != false || orders["frontier"] != uint64(8) || orders["observed"] != uint64(10) || orders["lag"] != uint64(2) || orders["error_code"] != "timeout" {
		t.Errorf("orders status = %#v, want normalized snapshot values", orders)
	}
	people := result.Rows[1]
	if people["source"] != "people" || people["state"] != CatalogSourceStateUnknown || people["available"] != false || people["ready"] != false || people["error_code"] != "" {
		t.Errorf("people status = %#v, want unknown safe defaults", people)
	}

	for _, shortcut := range []string{"SHOW SOURCE STATUS", "SHOW SOURCE_STATUS"} {
		compiled, err := CompileSQLShortcut(shortcut)
		if err != nil || compiled == shortcut {
			t.Fatalf("CompileSQLShortcut(%q) = %q, %v", shortcut, compiled, err)
		}
	}
}

type mu015FrontierSource struct{}

func (mu015FrontierSource) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (mu015FrontierSource) SQLSourceFrontier(name, key string) (uint64, bool, bool, error) {
	if name == "CACHE" && key == "orders" {
		return 12, true, true, nil
	}
	return 0, false, false, nil
}

func TestMU015CatalogResolverUsesExistingSourceFrontierContract(t *testing.T) {
	resolver := CatalogResolver{
		Source: mu015FrontierSource{},
		Catalog: Catalog{Sources: []CatalogSource{{
			Namespace: "public",
			Name:      "orders",
			Kind:      "CACHE",
		}}},
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SHOW SOURCE STATUS", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("frontier-derived status query error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["state"] != CatalogSourceStateReady || result.Rows[0]["available"] != true || result.Rows[0]["ready"] != true || result.Rows[0]["frontier"] != uint64(12) {
		t.Fatalf("frontier-derived status = %#v, want ready frontier 12", result.Rows)
	}
	frontier, ready, available, err := resolver.SQLSourceFrontier("CACHE", "orders")
	if err != nil || frontier != 12 || !ready || !available {
		t.Fatalf("CatalogResolver.SQLSourceFrontier() = %d/%v/%v/%v, want 12/true/true/nil", frontier, ready, available, err)
	}
}

func TestMU015CatalogSourceStatusRejectsUnsafeErrorCodesAndInconsistentLag(t *testing.T) {
	tests := []CatalogSourceStatus{
		{Namespace: "public", Source: "orders", Kind: "CACHE", State: CatalogSourceStateFailed, ErrorCode: "password=secret"},
		{Namespace: "public", Source: "orders", Kind: "CACHE", State: CatalogSourceStateRunning, Frontier: 8, Observed: 10, Lag: 1},
	}
	for index, status := range tests {
		resolver := CatalogResolver{Catalog: Catalog{SourceStatuses: []CatalogSourceStatus{status}}}
		_, err := ExecuteSQLQueryParameters(context.Background(), "SHOW SOURCE STATUS", resolver, nil, SQLQueryOptions{})
		if !errors.Is(err, ErrCatalogSourceStatusInvalid) {
			t.Errorf("case %d error = %v, want ErrCatalogSourceStatusInvalid", index, err)
		}
	}
}
