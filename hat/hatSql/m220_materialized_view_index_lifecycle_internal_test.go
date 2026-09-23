package hatSql

import (
	"context"
	"testing"
	"time"
)

func TestM220DropPointLookupFieldsWaitsForReaderDrain(t *testing.T) {
	views := NewMaterializedViews()
	resolver := SourceResolverFunc(func(_ string, _ string) ([]Row, error) {
		return []Row{{"id": int64(1), "region": "sg"}}, nil
	})
	if _, err := views.Create(context.Background(), MaterializedViewDefinition{
		Name:              "people_view",
		Query:             "FROM CACHE('people') SELECT id, region",
		Dependencies:      []string{"people"},
		PointLookupFields: []string{"region"},
	}, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}

	views.mu.RLock()
	view := views.views["people_view"]
	views.mu.RUnlock()
	view.readGate.mu.RLock()
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		result <- views.DropPointLookupFields("people_view", "region")
	}()
	<-started
	select {
	case err := <-result:
		t.Fatalf("DropPointLookupFields returned before reader drained: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	view.readGate.mu.RUnlock()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
