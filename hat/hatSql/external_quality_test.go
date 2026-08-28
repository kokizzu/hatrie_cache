package hatSql

import (
	"strings"
	"testing"
	"time"
)

func TestCopyCSVValidatesQuarantinesAndAddsProvenance(t *testing.T) {
	tables := NewExternalTables()
	report, err := tables.CopyCSV("orders", []byte("id,total\na,10\nb,nope\n,12\n"), SourceImportOptions{
		Source: "orders-v1", KeyColumn: "id", Version: "v1", IngestedAt: time.Unix(123, 0).UTC(), Quarantine: "orders_rejected",
		Schema: SourceSchema{Mode: SourceSchemaStrict, Fields: map[string]SourceFieldSchema{"id": {Type: SourceTypeString, Required: true}, "total": {Type: SourceTypeInteger, Required: true}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Accepted != 1 || report.Rejected != 2 || len(report.Validation) != 2 {
		t.Fatalf("report = %#v", report)
	}
	if got := string(report.ErrorCSV); !strings.Contains(got, "3,total,expected integer") || !strings.Contains(got, "4,id,required value is missing") {
		t.Fatalf("error CSV = %q", got)
	}
	accepted, ok := tables.Get("orders")
	if !ok || accepted.Rows[0]["_source"] != "orders-v1" || accepted.Rows[0]["_key"] != "a" || accepted.Rows[0]["_version"] != "v1" {
		t.Fatalf("accepted = %#v", accepted)
	}
	quarantine, ok := tables.Get("orders_rejected")
	if !ok || len(quarantine.Rows) != 2 {
		t.Fatalf("quarantine = %#v", quarantine)
	}
}
