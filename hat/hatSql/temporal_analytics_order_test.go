package hatSql

import (
	"testing"
	"time"
)

func TestTemporalTableAsOfOrdersOutOfOrderAndReturnsIndependentRows(t *testing.T) {
	table := NewTemporalTable()
	base := time.Unix(0, 0).UTC()
	table.Upsert("account-1", base.Add(2*time.Minute), Row{"value": "late"})
	table.Upsert("account-1", base, Row{"value": "first"})
	table.Upsert("account-1", base.Add(time.Minute), Row{"value": "original"})
	table.Upsert("account-1", base.Add(time.Minute), Row{"value": "replacement"})

	row, found := table.AsOf("account-1", base.Add(30*time.Second))
	if !found || row["value"] != "first" {
		t.Fatalf("early AS OF = %#v, found %t", row, found)
	}
	row, found = table.AsOf("account-1", base.Add(time.Minute))
	if !found || row["value"] != "replacement" {
		t.Fatalf("equal-time AS OF = %#v, found %t", row, found)
	}
	row["value"] = "mutated"
	row, found = table.AsOf("account-1", base.Add(time.Minute))
	if !found || row["value"] != "replacement" {
		t.Fatalf("AS OF copy isolation = %#v, found %t", row, found)
	}
}
