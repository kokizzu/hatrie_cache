package hatSql

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestMZ049SchemaDriftQuarantinesRowsAndContinues(t *testing.T) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryInt64},
		{Name: "name", Type: SQLRowBinaryString, Nullable: true},
	}
	rows := []Row{
		{"id": int64(1), "name": "ok"},
		{"id": "bad", "name": "wrong type"},
		{"id": int64(3), "extra": true},
		{"name": "missing id"},
		{"id": int64(5), "name": nil},
	}
	accepted := make([]int64, 0)
	quarantined := make([]ExternalSchemaQuarantineRecord, 0)
	stats, err := QuarantineExternalRows(rows, columns, ExternalSchemaQuarantineOptions{}, func(row Row) error {
		accepted = append(accepted, row["id"].(int64))
		return nil
	}, func(record ExternalSchemaQuarantineRecord) error {
		quarantined = append(quarantined, record)
		return nil
	})
	if err != nil {
		t.Fatalf("QuarantineExternalRows() error = %v", err)
	}
	if !reflect.DeepEqual(accepted, []int64{1, 5}) {
		t.Fatalf("accepted = %#v, want [1 5]", accepted)
	}
	if stats.AcceptedRows != 2 || stats.QuarantinedRows != 3 {
		t.Fatalf("stats = %#v, want 2 accepted and 3 quarantined", stats)
	}
	if len(quarantined) != 3 || quarantined[0].RowNumber != 2 || quarantined[1].RowNumber != 3 || quarantined[2].RowNumber != 4 {
		t.Fatalf("quarantine row order = %#v, want rows 2, 3, 4", quarantined)
	}
	if quarantined[0].Issues[0].Column != "id" || quarantined[0].Issues[0].Actual != SQLRowBinaryString {
		t.Fatalf("type drift = %#v, want id/string issue", quarantined[0].Issues)
	}
	if quarantined[1].Issues[0].Column != "extra" || quarantined[1].Issues[0].Reason != "unknown column" {
		t.Fatalf("unknown-column drift = %#v", quarantined[1].Issues)
	}
	if quarantined[2].Issues[0].Column != "id" || quarantined[2].Issues[0].Reason != "missing required column" {
		t.Fatalf("missing-column drift = %#v", quarantined[2].Issues)
	}
}

func TestMZ049SchemaDriftAllowsNullableAndNumericPromotion(t *testing.T) {
	columns := []SQLRowBinaryColumn{
		{Name: "id", Type: SQLRowBinaryFloat64},
		{Name: "optional", Type: SQLRowBinaryString, Nullable: true},
	}
	rows := []Row{
		{"id": int64(1)},
		{"id": uint64(2), "optional": nil},
		{"id": float64(3.5), "optional": "present"},
	}
	stats, err := QuarantineExternalRows(rows, columns, ExternalSchemaQuarantineOptions{AllowNumericPromotion: true}, func(Row) error {
		return nil
	}, func(record ExternalSchemaQuarantineRecord) error {
		t.Fatalf("unexpected quarantine record = %#v", record)
		return nil
	})
	if err != nil {
		t.Fatalf("QuarantineExternalRows() error = %v", err)
	}
	if stats.AcceptedRows != len(rows) || stats.QuarantinedRows != 0 {
		t.Fatalf("stats = %#v, want all rows accepted", stats)
	}
}

func TestMZ049SchemaDriftStreamsJSONEachRow(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}}
	accepted := make([]int64, 0)
	quarantined := make([]ExternalSchemaQuarantineRecord, 0)
	stats, err := QuarantineExternalJSONEachRow(strings.NewReader("{\"id\":1}\n{\"id\":\"bad\"}\n{\"id\":3}\n"), ExternalImportOptions{}, columns, ExternalSchemaQuarantineOptions{}, func(row Row) error {
		id, err := row["id"].(json.Number).Int64()
		if err != nil {
			return err
		}
		accepted = append(accepted, id)
		return nil
	}, func(record ExternalSchemaQuarantineRecord) error {
		quarantined = append(quarantined, record)
		return nil
	})
	if err != nil {
		t.Fatalf("QuarantineExternalJSONEachRow() error = %v", err)
	}
	if !reflect.DeepEqual(accepted, []int64{1, 3}) || len(quarantined) != 1 || quarantined[0].RowNumber != 2 {
		t.Fatalf("stream result = accepted %#v, quarantined %#v, stats %#v", accepted, quarantined, stats)
	}
	if stats.AcceptedRows != 2 || stats.QuarantinedRows != 1 {
		t.Fatalf("stream stats = %#v, want 2 accepted and 1 quarantined", stats)
	}
}

func TestMZ049SchemaDriftRejectsInvalidInputsAndBounds(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryInt64}}
	rows := []Row{{"id": int64(1)}, {"id": "bad"}, {"id": "bad again"}}
	for name, options := range map[string]ExternalSchemaQuarantineOptions{
		"negative MaxRows":            {MaxRows: -1},
		"negative MaxQuarantinedRows": {MaxQuarantinedRows: -1},
		"negative MaxIssuesPerRow":    {MaxIssuesPerRow: -1},
		"large MaxRows":               {MaxRows: maxExternalSchemaQuarantineRows + 1},
		"large MaxQuarantinedRows":    {MaxQuarantinedRows: maxExternalSchemaQuarantineRows + 1},
		"large MaxIssuesPerRow":       {MaxIssuesPerRow: maxExternalSchemaQuarantineIssues + 1},
	} {
		if _, err := QuarantineExternalRows(rows, columns, options, func(Row) error { return nil }, func(ExternalSchemaQuarantineRecord) error { return nil }); err == nil {
			t.Fatalf("%s error = nil", name)
		}
	}
	if _, err := QuarantineExternalRows(rows, columns, ExternalSchemaQuarantineOptions{MaxQuarantinedRows: 0}, nil, func(ExternalSchemaQuarantineRecord) error { return nil }); err == nil {
		t.Fatal("nil accept callback error = nil")
	}
	if _, err := QuarantineExternalRows(rows, columns, ExternalSchemaQuarantineOptions{}, func(Row) error { return nil }, nil); err == nil {
		t.Fatal("nil quarantine callback error = nil")
	}
	if _, err := QuarantineExternalRows(rows, columns, ExternalSchemaQuarantineOptions{MaxQuarantinedRows: 1}, func(Row) error { return nil }, func(ExternalSchemaQuarantineRecord) error { return nil }); !errors.Is(err, ErrExternalSchemaQuarantineLimit) {
		t.Fatalf("quarantine limit error = %v, want %v", err, ErrExternalSchemaQuarantineLimit)
	}
	if _, err := QuarantineExternalRows(rows, columns, ExternalSchemaQuarantineOptions{}, func(Row) error { return errors.New("stop") }, func(ExternalSchemaQuarantineRecord) error { return nil }); err == nil {
		t.Fatal("accept callback error = nil")
	}
	if _, err := QuarantineExternalRows(rows, columns, ExternalSchemaQuarantineOptions{}, func(Row) error { return nil }, func(ExternalSchemaQuarantineRecord) error { return errors.New("stop") }); err == nil {
		t.Fatal("quarantine callback error = nil")
	}
	if _, err := QuarantineExternalRows(rows, []SQLRowBinaryColumn{{Name: "id", Type: SQLRowBinaryType(255)}}, ExternalSchemaQuarantineOptions{}, func(Row) error { return nil }, func(ExternalSchemaQuarantineRecord) error { return nil }); err == nil {
		t.Fatal("invalid schema error = nil")
	}
	stats, err := QuarantineExternalRows([]Row{{"payload": json.RawMessage("{")}}, []SQLRowBinaryColumn{{Name: "payload", Type: SQLRowBinaryJSON}}, ExternalSchemaQuarantineOptions{}, func(Row) error { return nil }, func(ExternalSchemaQuarantineRecord) error { return nil })
	if err != nil || stats.QuarantinedRows != 1 {
		t.Fatalf("invalid JSON payload result = %#v/%v, want one quarantined row", stats, err)
	}
	stats, err = QuarantineExternalRows([]Row{{"payload": json.Number("not-a-number")}}, []SQLRowBinaryColumn{{Name: "payload", Type: SQLRowBinaryJSON}}, ExternalSchemaQuarantineOptions{}, func(Row) error { return nil }, func(ExternalSchemaQuarantineRecord) error { return nil })
	if err != nil || stats.QuarantinedRows != 1 {
		t.Fatalf("invalid JSON number result = %#v/%v, want one quarantined row", stats, err)
	}
}
