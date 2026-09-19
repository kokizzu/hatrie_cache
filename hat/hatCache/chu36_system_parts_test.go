package hatCache

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestSQLSystemPartsProviderExposesStableMetadata(t *testing.T) {
	created := time.Date(2026, 9, 19, 1, 2, 3, 0, time.UTC)
	retention := created.Add(24 * time.Hour)
	provider := SQLSystemPartProviderFunc(func() ([]SQLSystemPart, error) {
		return []SQLSystemPart{
			{
				Name:           "part-b",
				Partition:      2,
				Rows:           20,
				BytesOnDisk:    2048,
				Active:         true,
				State:          "active",
				Level:          3,
				DataVersion:    7,
				MinKey:         "b",
				MaxKey:         "z",
				Checksum:       "sha256:b",
				CreatedAt:      created,
				RetentionUntil: retention,
			},
			{
				Name:        "part-a",
				Partition:   1,
				Rows:        10,
				BytesOnDisk: 1024,
				Active:      false,
				State:       "obsolete",
				Level:       1,
				DataVersion: 6,
				Checksum:    "sha256:a",
			},
		}, nil
	})
	resolver := NewSQLSystemTablesResolver(nil, SQLSystemTablesResolverOptions{
		PartProvider: provider,
	})

	rows, err := resolver.ResolveSQLSource("CACHE", SQLSystemPartsTable)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %#v, want two rows", rows)
	}
	if rows[0]["name"] != "part-a" || rows[1]["name"] != "part-b" {
		t.Fatalf("rows are not deterministic: %#v", rows)
	}
	want := SQLRow{
		"name":            "part-b",
		"partition":       int64(2),
		"rows":            int64(20),
		"bytes_on_disk":   int64(2048),
		"active":          true,
		"state":           "active",
		"level":           int64(3),
		"data_version":    int64(7),
		"min_key":         "b",
		"max_key":         "z",
		"checksum":        "sha256:b",
		"created_at":      created,
		"retention_until": retention,
	}
	if !reflect.DeepEqual(rows[1], want) {
		t.Fatalf("row = %#v, want %#v", rows[1], want)
	}
	if _, exposed := rows[1]["path"]; exposed {
		t.Fatalf("system parts must not expose filesystem paths: %#v", rows[1])
	}
}

func TestSQLSystemPartsProviderRejectsInvalidAndUnboundedRows(t *testing.T) {
	tests := []struct {
		name    string
		parts   []SQLSystemPart
		limit   int
		wantErr error
	}{
		{
			name:    "limit",
			parts:   []SQLSystemPart{{Name: "a"}, {Name: "b"}},
			limit:   1,
			wantErr: ErrSQLSystemPartsLimitExceeded,
		},
		{
			name:    "negative rows",
			parts:   []SQLSystemPart{{Name: "a", Rows: -1}},
			wantErr: ErrSQLSystemPartInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := NewSQLSystemTablesResolver(nil, SQLSystemTablesResolverOptions{
				PartProvider: SQLSystemPartProviderFunc(func() ([]SQLSystemPart, error) {
					return test.parts, nil
				}),
				PartLimit: test.limit,
			})
			_, err := resolver.ResolveSQLSource("CACHE", SQLSystemPartsTable)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestSQLSystemPartsProviderPropagatesSnapshotError(t *testing.T) {
	want := errors.New("parts unavailable")
	resolver := NewSQLSystemTablesResolver(nil, SQLSystemTablesResolverOptions{
		PartProvider: SQLSystemPartProviderFunc(func() ([]SQLSystemPart, error) {
			return nil, want
		}),
	})
	_, err := resolver.ResolveSQLSource("CACHE", SQLSystemPartsTable)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}
