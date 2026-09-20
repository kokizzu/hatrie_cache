# T-U25 R-tree Space Catalog

`hatDataStructure.RTreeSpaceCatalog[T]` is an opt-in lifecycle wrapper around the existing concurrent `RTree`. It gives a named spatial index a stable definition, write-maintained membership, planner-visible metadata, and an atomic rebuild path.

## Usage

```go
type Place struct {
	Indexed bool
	X       float64
	Y       float64
}

catalog := hatDataStructure.NewRTreeSpaceCatalog[Place]()
err := catalog.Create(hatDataStructure.RTreeSpaceIndexDefinition[Place]{
	Name:       "places",
	BoundsName: "point",
	BoundsExtractor: func(place Place) (hatDataStructure.RTreeBounds, bool, error) {
		if !place.Indexed {
			return hatDataStructure.RTreeBounds{}, false, nil
		}
		return hatDataStructure.RTreeBounds{
			MinX: place.X,
			MinY: place.Y,
			MaxX: place.X,
			MaxY: place.Y,
		}, true, nil
	},
})
if err != nil {
		return err
}

_ = catalog.Upsert("places", 42, Place{Indexed: true, X: 10, Y: 20})
ids, _ := catalog.Search("places", hatDataStructure.RTreeBounds{
	MinX: 9, MinY: 19, MaxX: 11, MaxY: 21,
})
// ids is []uint64{42}.

// Changing Indexed to false removes the row from the named index.
_ = catalog.Upsert("places", 42, Place{Indexed: false})
```

`Upsert` extracts bounds before changing the tree. Invalid bounds or extractor errors leave the previous membership unchanged. A `false` extractor result removes the ID, which handles predicate transitions without a separate delete path. `Delete` reports whether an indexed ID existed.

## Rebuild and planning

```go
err := catalog.Rebuild("places", []hatDataStructure.RTreeSpaceRow[Place]{
	{ID: 7, Value: Place{Indexed: true, X: 3, Y: 4}},
})
```

Rebuild constructs a replacement tree from the caller's row snapshot. The old tree remains readable while it is built. Once rebuilding starts, writes, deletes, drops, and a second rebuild return `ErrRTreeSpaceCatalogRebuilding`; the replacement is installed under one lock transition. A failed extraction or invalid bounds restores `ready` state and keeps the old tree and generation. Successful replacements increment `Generation`.

`Metadata` and `ListMetadata` expose the index name, bounds identity, lifecycle state, generation, entry count, and configured node fanout for a planner or diagnostics surface. Search results remain sorted by ID because the underlying `RTree` contract is preserved.

The catalog is intentionally opt-in. SQL row routing, predicate determinism, and snapshot acquisition remain caller-owned so existing tuple writes and query planning keep their current behavior.

## Cost

The catalog adds one named-index lookup, extractor call, and catalog read lock around each maintained write/search. The measured steady-state overhead was approximately 2% for updates and 4% for searches, with no additional allocations or bytes per operation. Rebuild cost is proportional to the supplied snapshot and is intentionally explicit rather than hidden in a foreground write.

Reproduce the measurements with `make benchmark-tu25`.
