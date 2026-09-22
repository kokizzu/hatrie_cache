package hatSql_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func m220CreatePointLookupIndex(t *testing.T) *hatSql.MaterializedViews {
	t.Helper()
	source := &m219BackgroundSource{rows: []hatSql.Row{
		{"id": int64(1), "name": "one"},
		{"id": int64(2), "name": "two"},
	}}
	views := m219CreatePeopleView(t, source)
	if err := views.CreatePointLookupIndex(m219PointLookupDefinition(func(row hatSql.Row) (string, error) {
		return fmt.Sprint(row["id"]), nil
	})); err != nil {
		t.Fatalf("CreatePointLookupIndex() error = %v", err)
	}
	return views
}

func TestM220PointLookupRetirementWaitsForReaderDrain(t *testing.T) {
	views := m220CreatePointLookupIndex(t)
	reader, err := views.AcquirePointLookupReader("people_by_id")
	if err != nil {
		t.Fatalf("AcquirePointLookupReader() error = %v", err)
	}
	defer reader.Close()

	retirement, err := views.StartPointLookupIndexRetirement("people_by_id")
	if err != nil {
		t.Fatalf("StartPointLookupIndexRetirement() error = %v", err)
	}
	status := retirement.Status()
	if status.State != hatSql.MaterializedViewPointLookupRetirementStateRetiring || status.ActiveReaders != 1 {
		t.Fatalf("retirement status = %#v, want retiring with one reader", status)
	}
	if _, found, err := views.LookupPoint("people_by_id", "1"); !errors.Is(err, hatSql.ErrMaterializedViewPointLookupIndexMissing) || found {
		t.Fatalf("LookupPoint() during retirement = found %v, err %v, want missing", found, err)
	}
	result, found, err := reader.LookupPoint("1")
	if err != nil || !found || len(result.Rows) != 1 || result.Rows[0]["name"] != "one" {
		t.Fatalf("reader.LookupPoint() during retirement = %#v, %v, %v", result, found, err)
	}

	waitContext, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	deferredStatus, waitErr := retirement.Wait(waitContext)
	cancel()
	if !errors.Is(waitErr, context.DeadlineExceeded) || deferredStatus.ActiveReaders != 1 {
		t.Fatalf("retirement.Wait() before close = %#v, %v, want timeout with one reader", deferredStatus, waitErr)
	}

	reader.Close()
	finalStatus, err := retirement.Wait(context.Background())
	if err != nil || finalStatus.State != hatSql.MaterializedViewPointLookupRetirementStateRetired || finalStatus.ActiveReaders != 0 {
		t.Fatalf("retirement.Wait() after close = %#v, %v, want retired", finalStatus, err)
	}
	if _, _, err := reader.LookupPoint("1"); !errors.Is(err, hatSql.ErrMaterializedViewPointLookupReaderClosed) {
		t.Fatalf("reader.LookupPoint() after close error = %v, want reader-closed", err)
	}
}

func TestM220DropPointLookupIndexWaitsForReaderDrain(t *testing.T) {
	views := m220CreatePointLookupIndex(t)
	reader, err := views.AcquirePointLookupReader("people_by_id")
	if err != nil {
		t.Fatalf("AcquirePointLookupReader() error = %v", err)
	}
	dropped := make(chan error, 1)
	go func() {
		dropped <- views.DropPointLookupIndex("people_by_id")
	}()
	select {
	case err := <-dropped:
		t.Fatalf("DropPointLookupIndex() returned before reader close: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, found, err := reader.LookupPoint("2"); err != nil || !found {
		t.Fatalf("reader.LookupPoint() before drop completes = found %v, err %v", found, err)
	}
	reader.Close()
	select {
	case err := <-dropped:
		if err != nil {
			t.Fatalf("DropPointLookupIndex() after reader close error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DropPointLookupIndex() did not finish after reader close")
	}
	if _, found, err := views.LookupPoint("people_by_id", "2"); !errors.Is(err, hatSql.ErrMaterializedViewPointLookupIndexMissing) || found {
		t.Fatalf("LookupPoint() after drop = found %v, err %v, want missing", found, err)
	}
}
