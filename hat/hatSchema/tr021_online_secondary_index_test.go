package hatSchema

import (
	"errors"
	"sync"
	"testing"
)

func TestMaterializedSourceBuildsSecondaryIndexOnline(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id"},
		{Name: "region"},
		{Name: "payload"},
	})
	for index := 0; index < 100; index++ {
		if _, err := source.Insert(Row{"id": int64(index), "region": "region-a", "payload": index}); err != nil {
			t.Fatal(err)
		}
	}
	if source.HasIndex("region") {
		t.Fatal("region index exists before the online build")
	}
	report, err := source.BuildSecondaryIndex("region")
	if err != nil {
		t.Fatalf("BuildSecondaryIndex() error = %v", err)
	}
	if report.Field != "region" || report.Rows != 100 || report.Attempts < 1 {
		t.Fatalf("BuildSecondaryIndex() report = %#v", report)
	}
	if !source.HasIndex("region") {
		t.Fatal("region index was not installed")
	}
	if rows := source.Lookup("region", "region-a"); len(rows) != 100 {
		t.Fatalf("Lookup() returned %d rows after build, want 100", len(rows))
	}
	if _, err := source.Insert(Row{"id": int64(100), "region": "region-a", "payload": 100}); err != nil {
		t.Fatal(err)
	}
	if rows := source.Lookup("region", "region-a"); len(rows) != 101 {
		t.Fatalf("Lookup() returned %d rows after indexed insert, want 101", len(rows))
	}
	if _, err := source.BuildSecondaryIndex("missing"); !errors.Is(err, ErrMaterializedSourceColumnUnknown) {
		t.Fatalf("BuildSecondaryIndex(missing) error = %v, want unknown-column error", err)
	}
}

func TestMaterializedSourceOnlineIndexBuildKeepsConcurrentInserts(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "region"}})
	for index := 0; index < 1_000; index++ {
		if _, err := source.Insert(Row{"id": int64(index), "region": "seed"}); err != nil {
			t.Fatal(err)
		}
	}
	var buildErr error
	var buildReport SecondaryIndexBuildReport
	var writerErr error
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		buildReport, buildErr = source.BuildSecondaryIndex("region")
	}()
	go func() {
		defer group.Done()
		for index := 1_000; index < 2_000; index++ {
			if _, err := source.Insert(Row{"id": int64(index), "region": "concurrent"}); err != nil {
				writerErr = err
				return
			}
		}
	}()
	group.Wait()
	if buildErr != nil || writerErr != nil {
		t.Fatalf("concurrent build/write errors = %v/%v", buildErr, writerErr)
	}
	if buildReport.Rows < 1_000 || !source.HasIndex("region") {
		t.Fatalf("concurrent build report/index = %#v/%t", buildReport, source.HasIndex("region"))
	}
	if rows := source.Lookup("region", "concurrent"); len(rows) != 1_000 {
		t.Fatalf("concurrent indexed rows = %d, want 1000", len(rows))
	}
}

func TestSQLResolverAdapterUsesOnlyMaintainedSecondaryIndexes(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "region"}})
	if _, err := source.Insert(Row{"id": int64(1), "region": "sg"}); err != nil {
		t.Fatal(err)
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"people": source}}
	if rows, available, err := adapter.ResolveSQLIndexedSource("CACHE", "people", "region", "sg"); err != nil || available || rows != nil {
		t.Fatalf("unbuilt indexed lookup = %#v/%t/%v, want unavailable", rows, available, err)
	}
	if _, err := source.BuildSecondaryIndex("region"); err != nil {
		t.Fatal(err)
	}
	rows, available, err := adapter.ResolveSQLIndexedSource("CACHE", "people", "region", "sg")
	if err != nil || !available || len(rows) != 1 {
		t.Fatalf("built indexed lookup = %#v/%t/%v, want one available row", rows, available, err)
	}
}
