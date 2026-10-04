package hatSchema

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOnlineSpaceUpgradeKeepsReadsAndWritesAvailable(t *testing.T) {
	space, target := newOnlineSpaceFixture(t)
	if err := space.Upsert(Row{"id": "one", "name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	if err := space.Upsert(Row{"id": "two", "name": "Grace"}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var first uint32
	convert := func(ctx context.Context, row Row) error {
		if atomic.CompareAndSwapUint32(&first, 0, 1) {
			close(started)
			<-release
		}
		row["email"] = row["id"].(string) + "@example.test"
		return nil
	}
	upgrade, err := space.BeginUpgrade(context.Background(), target, convert)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("online upgrade did not start")
	}

	if got, ok := space.Get("one"); !ok || got["email"] != nil {
		t.Fatalf("active schema changed before cutover: %#v, %v", got, ok)
	}
	if err := space.Upsert(Row{"id": "three", "name": "Lin"}); err != nil {
		t.Fatalf("write during upgrade failed: %v", err)
	}
	if err := space.Upsert(Row{"id": "one", "name": "Ada-updated"}); err != nil {
		t.Fatalf("update during upgrade failed: %v", err)
	}
	if got, ok := space.Get("three"); !ok || got["email"] != nil {
		t.Fatalf("write was not visible in active schema: %#v, %v", got, ok)
	}
	if status := upgrade.Status(); status.Phase != OnlineSpaceUpgradeRunning {
		t.Fatalf("upgrade phase = %s, want running", status.Phase)
	}

	close(release)
	if err := upgrade.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := space.Definition().Version; got != target.Version {
		t.Fatalf("space version = %d, want %d", got, target.Version)
	}
	for _, key := range []string{"one", "two", "three"} {
		row, ok := space.Get(key)
		wantName := map[string]string{"one": "Ada-updated", "two": "Grace", "three": "Lin"}[key]
		if !ok || row["name"] != wantName || row["email"] != key+"@example.test" {
			t.Fatalf("converted row %q = %#v, %v", key, row, ok)
		}
	}
	if status := upgrade.Status(); status.Phase != OnlineSpaceUpgradeComplete || status.ConvertedRows != 3 || status.TotalRows != 3 {
		t.Fatalf("completion status = %#v", status)
	}
}

func TestOnlineSpaceUpgradeFailurePreservesCurrentRows(t *testing.T) {
	space, target := newOnlineSpaceFixture(t)
	if err := space.Upsert(Row{"id": "one", "name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("conversion failed")
	upgrade, err := space.BeginUpgrade(context.Background(), target, func(context.Context, Row) error {
		return wantErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := upgrade.Wait(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("Wait() error = %v, want %v", err, wantErr)
	}
	if status := upgrade.Status(); status.Phase != OnlineSpaceUpgradeFailed || !errors.Is(status.Err, wantErr) {
		t.Fatalf("failure status = %#v", status)
	}
	if got := space.Definition().Version; got != 1 {
		t.Fatalf("failed upgrade published version %d", got)
	}
	if row, ok := space.Get("one"); !ok || row["email"] != nil {
		t.Fatalf("failed upgrade changed row: %#v, %v", row, ok)
	}
	if err := space.Upsert(Row{"id": "two", "name": "Grace"}); err != nil {
		t.Fatalf("writes did not resume after failure: %v", err)
	}
}

func TestOnlineSpaceUpgradeCancellationPreservesCurrentRows(t *testing.T) {
	space, target := newOnlineSpaceFixture(t)
	if err := space.Upsert(Row{"id": "one", "name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	var first sync.Once
	convert := func(ctx context.Context, row Row) error {
		first.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	}
	upgrade, err := space.BeginUpgrade(context.Background(), target, convert)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("online upgrade did not start")
	}
	upgrade.Cancel()
	if err := upgrade.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context canceled", err)
	}
	if status := upgrade.Status(); status.Phase != OnlineSpaceUpgradeCanceled {
		t.Fatalf("cancellation status = %#v", status)
	}
	if got := space.Definition().Version; got != 1 {
		t.Fatalf("canceled upgrade published version %d", got)
	}
}

func TestOnlineSpaceUpgradeTracksDeletionDuringConversion(t *testing.T) {
	space, target := newOnlineSpaceFixture(t)
	if err := space.Upsert(Row{"id": "one", "name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	convert := func(_ context.Context, row Row) error {
		close(started)
		<-release
		row["email"] = "one@example.test"
		return nil
	}
	upgrade, err := space.BeginUpgrade(context.Background(), target, convert)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("online upgrade did not start")
	}
	if !space.Delete("one") {
		t.Fatal("row was not deleted")
	}
	close(release)
	if err := upgrade.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status := upgrade.Status(); status.Phase != OnlineSpaceUpgradeComplete || status.TotalRows != 0 || status.ConvertedRows != 0 {
		t.Fatalf("deletion status = %#v", status)
	}
	if rows := space.Rows(); len(rows) != 0 {
		t.Fatalf("rows after deletion = %#v", rows)
	}
}

func TestOnlineSpaceUpgradeRejectsUnsafeTarget(t *testing.T) {
	space, _ := newOnlineSpaceFixture(t)
	unsafe := SpaceDefinition{
		Name:    "users",
		Version: 2,
		Source:  Source{Name: "users", Columns: []Column{{Name: "id", Type: TypeInteger}}},
	}
	if _, err := space.BeginUpgrade(context.Background(), unsafe, func(context.Context, Row) error { return nil }); err == nil {
		t.Fatal("unsafe target was accepted")
	}
}

func newOnlineSpaceFixture(t *testing.T) (*OnlineSpace, SpaceDefinition) {
	t.Helper()
	initial := SpaceDefinition{
		Name:    "users",
		Version: 1,
		Source:  Source{Name: "users", Columns: []Column{{Name: "id", Type: TypeText}, {Name: "name", Type: TypeText}}},
	}
	target := initial
	target.Version = 2
	target.Source.Columns = append(append([]Column(nil), initial.Source.Columns...), Column{Name: "email", Type: TypeText})
	space, err := NewOnlineSpace(initial, func(row Row) (string, error) {
		id, ok := row["id"].(string)
		if !ok || id == "" {
			return "", errors.New("id is required")
		}
		return id, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return space, target
}
