package hatDataStructure

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

type tu25Record struct {
	Indexed bool
	X       float64
	Y       float64
}

func TestTU25RTreeSpaceCatalogMaintainsMembership(t *testing.T) {
	catalog := NewRTreeSpaceCatalog[tu25Record]()
	if err := catalog.Create(tu25RTreeDefinition()); err != nil {
		t.Fatalf("create: %v", err)
	}

	for id, record := range map[uint64]tu25Record{
		1: {Indexed: true, X: 1, Y: 1},
		2: {Indexed: false, X: 1, Y: 1},
		3: {Indexed: true, X: 2, Y: 2},
	} {
		if err := catalog.Upsert("geo", id, record); err != nil {
			t.Fatalf("upsert %d: %v", id, err)
		}
	}

	got, err := catalog.Search("geo", tu25PointBounds(1, 1))
	if err != nil {
		t.Fatalf("search initial: %v", err)
	}
	if want := []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("initial ids = %v, want %v", got, want)
	}

	if err := catalog.Upsert("geo", 1, tu25Record{Indexed: true, X: 2, Y: 2}); err != nil {
		t.Fatalf("move indexed row: %v", err)
	}
	got, err = catalog.Search("geo", tu25PointBounds(1, 1))
	if err != nil {
		t.Fatalf("search after move: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("old location ids = %v, want none", got)
	}
	got, err = catalog.Search("geo", tu25PointBounds(2, 2))
	if err != nil {
		t.Fatalf("search new location: %v", err)
	}
	if want := []uint64{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("new location ids = %v, want %v", got, want)
	}

	if err := catalog.Upsert("geo", 1, tu25Record{Indexed: false, X: 2, Y: 2}); err != nil {
		t.Fatalf("remove from index: %v", err)
	}
	got, err = catalog.Search("geo", tu25PointBounds(2, 2))
	if err != nil {
		t.Fatalf("search after predicate removal: %v", err)
	}
	if want := []uint64{3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after predicate removal ids = %v, want %v", got, want)
	}

	deleted, err := catalog.Delete("geo", 3)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted {
		t.Fatal("delete reported no change")
	}
	deleted, err = catalog.Delete("geo", 3)
	if err != nil {
		t.Fatalf("delete missing: %v", err)
	}
	if deleted {
		t.Fatal("second delete reported a change")
	}

	metadata, err := catalog.Metadata("geo")
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if metadata.State != RTreeSpaceReady || metadata.Generation != 1 || metadata.Entries != 0 {
		t.Fatalf("metadata = %+v, want ready generation 1 with no entries", metadata)
	}
}

func TestTU25RTreeSpaceCatalogPreservesOldValueOnInvalidBounds(t *testing.T) {
	catalog := NewRTreeSpaceCatalog[tu25Record]()
	definition := RTreeSpaceIndexDefinition[tu25Record]{
		Name:       "geo",
		BoundsName: "point",
		BoundsExtractor: func(record tu25Record) (RTreeBounds, bool, error) {
			if !record.Indexed {
				return RTreeBounds{}, false, nil
			}
			if record.X == 4 {
				return RTreeBounds{MinX: 5, MinY: 5, MaxX: 4, MaxY: 4}, true, nil
			}
			return tu25PointBounds(record.X, record.Y), true, nil
		},
	}
	if err := catalog.Create(definition); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := catalog.Upsert("geo", 1, tu25Record{Indexed: true, X: 1, Y: 1}); err != nil {
		t.Fatalf("initial upsert: %v", err)
	}
	if err := catalog.Upsert("geo", 1, tu25Record{Indexed: true, X: 4, Y: 4}); err == nil {
		t.Fatal("invalid bounds upsert succeeded")
	}

	got, err := catalog.Search("geo", tu25PointBounds(1, 1))
	if err != nil {
		t.Fatalf("search preserved value: %v", err)
	}
	if want := []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("preserved ids = %v, want %v", got, want)
	}
}

func TestTU25RTreeSpaceCatalogRebuildFencesWritesAndSwapsAtomically(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	catalog := NewRTreeSpaceCatalog[tu25Record]()
	definition := RTreeSpaceIndexDefinition[tu25Record]{
		Name:       "geo",
		BoundsName: "point",
		BoundsExtractor: func(record tu25Record) (RTreeBounds, bool, error) {
			if record.X == 20 {
				once.Do(func() {
					close(started)
					<-release
				})
			}
			return tu25PointBounds(record.X, record.Y), true, nil
		},
	}
	if err := catalog.Create(definition); err != nil {
		t.Fatalf("create: %v", err)
	}

	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- catalog.Rebuild("geo", []RTreeSpaceRow[tu25Record]{
			{ID: 2, Value: tu25Record{Indexed: true, X: 20, Y: 20}},
		})
	}()
	<-started

	metadata, err := catalog.Metadata("geo")
	if err != nil {
		t.Fatalf("rebuilding metadata: %v", err)
	}
	if metadata.State != RTreeSpaceRebuilding || metadata.Generation != 1 {
		t.Fatalf("rebuilding metadata = %+v", metadata)
	}
	if err := catalog.Upsert("geo", 3, tu25Record{Indexed: true, X: 30, Y: 30}); !errors.Is(err, ErrRTreeSpaceCatalogRebuilding) {
		t.Fatalf("upsert during rebuild error = %v, want %v", err, ErrRTreeSpaceCatalogRebuilding)
	}
	if err := catalog.Rebuild("geo", nil); !errors.Is(err, ErrRTreeSpaceCatalogRebuilding) {
		t.Fatalf("second rebuild error = %v, want %v", err, ErrRTreeSpaceCatalogRebuilding)
	}
	got, err := catalog.Search("geo", tu25PointBounds(20, 20))
	if err != nil {
		t.Fatalf("search during rebuild: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("new point visible before swap: %v", got)
	}

	close(release)
	if err := <-rebuildDone; err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	metadata, err = catalog.Metadata("geo")
	if err != nil {
		t.Fatalf("ready metadata: %v", err)
	}
	if metadata.State != RTreeSpaceReady || metadata.Generation != 2 || metadata.Entries != 1 {
		t.Fatalf("ready metadata = %+v", metadata)
	}
	got, err = catalog.Search("geo", tu25PointBounds(20, 20))
	if err != nil {
		t.Fatalf("search after rebuild: %v", err)
	}
	if want := []uint64{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rebuilt ids = %v, want %v", got, want)
	}
}

func TestTU25RTreeSpaceCatalogFailedRebuildPreservesReadyTree(t *testing.T) {
	extractorError := errors.New("bad row")
	catalog := NewRTreeSpaceCatalog[tu25Record]()
	definition := RTreeSpaceIndexDefinition[tu25Record]{
		Name:       "geo",
		BoundsName: "point",
		BoundsExtractor: func(record tu25Record) (RTreeBounds, bool, error) {
			if record.X == 99 {
				return RTreeBounds{}, false, extractorError
			}
			return tu25PointBounds(record.X, record.Y), record.Indexed, nil
		},
	}
	if err := catalog.Create(definition); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := catalog.Upsert("geo", 1, tu25Record{Indexed: true, X: 1, Y: 1}); err != nil {
		t.Fatalf("initial upsert: %v", err)
	}
	if err := catalog.Rebuild("geo", []RTreeSpaceRow[tu25Record]{
		{ID: 2, Value: tu25Record{Indexed: true, X: 99, Y: 99}},
	}); !errors.Is(err, extractorError) {
		t.Fatalf("failed rebuild error = %v, want %v", err, extractorError)
	}

	metadata, err := catalog.Metadata("geo")
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if metadata.State != RTreeSpaceReady || metadata.Generation != 1 || metadata.Entries != 1 {
		t.Fatalf("metadata after failed rebuild = %+v", metadata)
	}
	got, err := catalog.Search("geo", tu25PointBounds(1, 1))
	if err != nil {
		t.Fatalf("search after failed rebuild: %v", err)
	}
	if want := []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ids after failed rebuild = %v, want %v", got, want)
	}
}

func TestTU25RTreeSpaceCatalogValidatesLifecycle(t *testing.T) {
	catalog := NewRTreeSpaceCatalog[tu25Record]()
	if err := catalog.Create(RTreeSpaceIndexDefinition[tu25Record]{Name: "missing"}); !errors.Is(err, ErrRTreeSpaceCatalogDefinition) {
		t.Fatalf("missing extractor error = %v", err)
	}
	if err := catalog.Create(tu25RTreeDefinition()); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := catalog.Create(tu25RTreeDefinition()); !errors.Is(err, ErrRTreeSpaceCatalogExists) {
		t.Fatalf("duplicate error = %v", err)
	}
	if _, err := catalog.Metadata("missing"); !errors.Is(err, ErrRTreeSpaceCatalogNotFound) {
		t.Fatalf("missing metadata error = %v", err)
	}
	if _, err := catalog.Search("missing", tu25PointBounds(0, 0)); !errors.Is(err, ErrRTreeSpaceCatalogNotFound) {
		t.Fatalf("missing search error = %v", err)
	}

	if err := catalog.Create(RTreeSpaceIndexDefinition[tu25Record]{
		Name:            "zeta",
		BoundsName:      "point",
		BoundsExtractor: tu25RTreeDefinition().BoundsExtractor,
	}); err != nil {
		t.Fatalf("create zeta: %v", err)
	}
	metadata, err := catalog.ListMetadata()
	if err != nil {
		t.Fatalf("list metadata: %v", err)
	}
	if got := []string{metadata[0].Name, metadata[1].Name}; !reflect.DeepEqual(got, []string{"geo", "zeta"}) {
		t.Fatalf("metadata names = %v", got)
	}

	if err := catalog.Drop("zeta"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := catalog.Drop("zeta"); !errors.Is(err, ErrRTreeSpaceCatalogNotFound) {
		t.Fatalf("second drop error = %v", err)
	}
}

func tu25RTreeDefinition() RTreeSpaceIndexDefinition[tu25Record] {
	return RTreeSpaceIndexDefinition[tu25Record]{
		Name:       "geo",
		BoundsName: "point",
		BoundsExtractor: func(record tu25Record) (RTreeBounds, bool, error) {
			if !record.Indexed {
				return RTreeBounds{}, false, nil
			}
			return tu25PointBounds(record.X, record.Y), true, nil
		},
	}
}

func tu25PointBounds(x, y float64) RTreeBounds {
	return RTreeBounds{MinX: x, MinY: y, MaxX: x, MaxY: y}
}
