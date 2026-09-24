package hatSchema

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"hatrie_cache/hat/hatDataStructure"
	"hatrie_cache/hat/hatSql"
)

// SpatialIndexBuildReport describes one atomically published point R-tree.
// The index is opt-in and is maintained after publication by inserts and
// upserts. Rows with unusable coordinates remain in the fallback candidate
// set so SQL retains its ordinary NULL and error semantics.
type SpatialIndexBuildReport struct {
	LatitudeField  string
	LongitudeField string
	Rows           int
	IndexedRows    int
	FallbackRows   int
	Attempts       int
}

type materializedSpatialIndex struct {
	latitudeField  string
	longitudeField string
	tree           *hatDataStructure.RTree
	fallback       map[int]struct{}
}

// BuildSpatialIndex builds and atomically installs a point R-tree over the
// supplied latitude and longitude columns. The build runs outside the source
// write lock and retries if rows change before publication.
func (source *MaterializedSource) BuildSpatialIndex(latitudeField, longitudeField string) (SpatialIndexBuildReport, error) {
	if source == nil {
		return SpatialIndexBuildReport{}, ErrMaterializedSourceNil
	}
	latitudeField = strings.TrimSpace(latitudeField)
	longitudeField = strings.TrimSpace(longitudeField)
	if latitudeField == "" || longitudeField == "" {
		return SpatialIndexBuildReport{}, ErrMaterializedSourceColumnRequired
	}
	if latitudeField == longitudeField {
		return SpatialIndexBuildReport{}, fmt.Errorf("hatSchema: spatial index latitude and longitude fields must be distinct")
	}
	report := SpatialIndexBuildReport{LatitudeField: latitudeField, LongitudeField: longitudeField}
	for {
		source.mu.RLock()
		if !source.hasColumnLocked(latitudeField) {
			source.mu.RUnlock()
			return SpatialIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, latitudeField)
		}
		if !source.hasColumnLocked(longitudeField) {
			source.mu.RUnlock()
			return SpatialIndexBuildReport{}, fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, longitudeField)
		}
		generation := source.generation
		rows := append([]Row(nil), source.rows...)
		source.mu.RUnlock()

		tree, err := hatDataStructure.NewRTree(0)
		if err != nil {
			return SpatialIndexBuildReport{}, err
		}
		index := &materializedSpatialIndex{
			latitudeField:  latitudeField,
			longitudeField: longitudeField,
			tree:           tree,
			fallback:       make(map[int]struct{}),
		}
		indexedRows := 0
		for position, row := range rows {
			if latitude, longitude, ok := materializedSpatialPoint(row, latitudeField, longitudeField); ok {
				if err := tree.Upsert(uint64(position)+1, materializedSpatialBounds(latitude, longitude)); err != nil {
					return SpatialIndexBuildReport{}, err
				}
				indexedRows++
				continue
			}
			index.fallback[position] = struct{}{}
		}
		report.Attempts++

		source.mu.Lock()
		if source.generation != generation {
			source.mu.Unlock()
			continue
		}
		if source.spatialIndexes == nil {
			source.spatialIndexes = make(map[string]*materializedSpatialIndex)
		}
		source.spatialIndexes[spatialIndexKey(latitudeField, longitudeField)] = index
		source.mu.Unlock()
		report.Rows = len(rows)
		report.IndexedRows = indexedRows
		report.FallbackRows = len(rows) - indexedRows
		return report, nil
	}
}

// HasSpatialIndex reports whether the exact coordinate-field pair has an
// installed spatial index.
func (source *MaterializedSource) HasSpatialIndex(latitudeField, longitudeField string) bool {
	if source == nil {
		return false
	}
	latitudeField = strings.TrimSpace(latitudeField)
	longitudeField = strings.TrimSpace(longitudeField)
	source.mu.RLock()
	_, ok := source.spatialIndexes[spatialIndexKey(latitudeField, longitudeField)]
	source.mu.RUnlock()
	return ok
}

func (source *MaterializedSource) lookupSpatial(predicate hatSql.SQLGeoPredicate) ([]Row, bool, error) {
	if source == nil {
		return nil, false, nil
	}
	latitudeField := strings.TrimSpace(predicate.LatitudeField)
	longitudeField := strings.TrimSpace(predicate.LongitudeField)
	source.mu.RLock()
	index := source.spatialIndexes[spatialIndexKey(latitudeField, longitudeField)]
	if index == nil {
		source.mu.RUnlock()
		return nil, false, nil
	}
	bounds, err := materializedSpatialPredicateBounds(predicate)
	if err != nil {
		source.mu.RUnlock()
		return nil, true, err
	}
	positions, err := materializedSpatialSearch(index, bounds, len(source.rows))
	if err != nil {
		source.mu.RUnlock()
		return nil, true, err
	}
	rows := make([]Row, 0, len(positions))
	for _, position := range positions {
		rows = append(rows, cloneRow(source.rows[position]))
	}
	source.mu.RUnlock()
	return rows, true, nil
}

// ResolveSQLGeoSource exposes materialized-source spatial candidates to the
// SQL executor. The executor evaluates the original predicate again.
func (adapter SQLResolverAdapter) ResolveSQLGeoSource(name, key string, predicate hatSql.SQLGeoPredicate) ([]hatSql.Row, bool, error) {
	if strings.EqualFold(name, "CACHE") {
		if source := adapter.Sources[strings.ToLower(key)]; source != nil {
			rows, available, err := source.lookupSpatial(predicate)
			if !available {
				return nil, false, err
			}
			return sqlRows(rows), true, err
		}
	}
	if indexed, ok := adapter.Base.(hatSql.GeoIndexedSourceResolver); ok {
		return indexed.ResolveSQLGeoSource(name, key, predicate)
	}
	return nil, false, nil
}

func (source *MaterializedSource) addSpatialIndexesForInsertLocked(row Row, position int) {
	for _, index := range source.spatialIndexes {
		if index == nil || index.tree == nil {
			continue
		}
		if latitude, longitude, ok := materializedSpatialPoint(row, index.latitudeField, index.longitudeField); ok {
			_ = index.tree.Upsert(uint64(position)+1, materializedSpatialBounds(latitude, longitude))
			delete(index.fallback, position)
			continue
		}
		index.fallback[position] = struct{}{}
	}
}

func (source *MaterializedSource) replaceSpatialIndexesLocked(before, after Row, position int) {
	for _, index := range source.spatialIndexes {
		if index == nil || index.tree == nil {
			continue
		}
		index.tree.Delete(uint64(position) + 1)
		delete(index.fallback, position)
		if latitude, longitude, ok := materializedSpatialPoint(after, index.latitudeField, index.longitudeField); ok {
			_ = index.tree.Upsert(uint64(position)+1, materializedSpatialBounds(latitude, longitude))
			continue
		}
		index.fallback[position] = struct{}{}
	}
}

func spatialIndexKey(latitudeField, longitudeField string) string {
	return latitudeField + "\x00" + longitudeField
}

func materializedSpatialPoint(row Row, latitudeField, longitudeField string) (float64, float64, bool) {
	latitude, latitudeOK := materializedSpatialNumber(row[latitudeField])
	longitude, longitudeOK := materializedSpatialNumber(row[longitudeField])
	if !latitudeOK || !longitudeOK || math.IsNaN(latitude) || math.IsInf(latitude, 0) || latitude < -90 || latitude > 90 || math.IsNaN(longitude) || math.IsInf(longitude, 0) || longitude < -180 || longitude > 180 {
		return 0, 0, false
	}
	return latitude, longitude, true
}

func materializedSpatialNumber(value interface{}) (float64, bool) {
	switch value := value.(type) {
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

func materializedSpatialBounds(latitude, longitude float64) hatDataStructure.RTreeBounds {
	return hatDataStructure.RTreeBounds{MinX: longitude, MinY: latitude, MaxX: longitude, MaxY: latitude}
}

func materializedSpatialPredicateBounds(predicate hatSql.SQLGeoPredicate) (hatSql.GeoBoundingBox, error) {
	switch predicate.Kind {
	case hatSql.SQLGeoPredicateWithinBox:
		bounds := predicate.Bounds
		if !materializedSpatialCoordinateValid(bounds.MinLatitude, bounds.MinLongitude) || !materializedSpatialCoordinateValid(bounds.MaxLatitude, bounds.MaxLongitude) || bounds.MinLatitude > bounds.MaxLatitude {
			return hatSql.GeoBoundingBox{}, fmt.Errorf("invalid geo bounding box")
		}
		return bounds, nil
	case hatSql.SQLGeoPredicateWithinRadius:
		if !materializedSpatialCoordinateValid(predicate.Center.Latitude, predicate.Center.Longitude) || math.IsNaN(predicate.RadiusMeters) || math.IsInf(predicate.RadiusMeters, 0) || predicate.RadiusMeters < 0 {
			return hatSql.GeoBoundingBox{}, fmt.Errorf("invalid geo radius predicate")
		}
		return materializedSpatialRadiusBounds(predicate.Center.Latitude, predicate.Center.Longitude, predicate.RadiusMeters), nil
	default:
		return hatSql.GeoBoundingBox{}, fmt.Errorf("unsupported geo predicate kind %d", predicate.Kind)
	}
}

func materializedSpatialCoordinateValid(latitude, longitude float64) bool {
	return !math.IsNaN(latitude) && !math.IsInf(latitude, 0) && latitude >= -90 && latitude <= 90 && !math.IsNaN(longitude) && !math.IsInf(longitude, 0) && longitude >= -180 && longitude <= 180
}

func materializedSpatialRadiusBounds(latitude, longitude, radiusMeters float64) hatSql.GeoBoundingBox {
	const earthRadiusMeters = 6_371_008.8
	angularDistance := math.Min(math.Pi, radiusMeters/earthRadiusMeters)
	latitudeDelta := angularDistance * 180 / math.Pi
	minimumLatitude := math.Max(-90, latitude-latitudeDelta)
	maximumLatitude := math.Min(90, latitude+latitudeDelta)
	if minimumLatitude == -90 || maximumLatitude == 90 {
		return hatSql.GeoBoundingBox{MinLatitude: minimumLatitude, MaxLatitude: maximumLatitude, MinLongitude: -180, MaxLongitude: 180}
	}
	longitudeDelta := math.Asin(math.Min(1, math.Sin(angularDistance)/math.Cos(latitude*math.Pi/180))) * 180 / math.Pi
	minimumLongitude := longitude - longitudeDelta
	maximumLongitude := longitude + longitudeDelta
	if minimumLongitude < -180 {
		minimumLongitude += 360
	}
	if maximumLongitude > 180 {
		maximumLongitude -= 360
	}
	return hatSql.GeoBoundingBox{MinLatitude: minimumLatitude, MaxLatitude: maximumLatitude, MinLongitude: minimumLongitude, MaxLongitude: maximumLongitude}
}

func materializedSpatialSearch(index *materializedSpatialIndex, bounds hatSql.GeoBoundingBox, rowCount int) ([]int, error) {
	search := func(minLongitude, maxLongitude float64) ([]uint64, error) {
		return index.tree.Search(hatDataStructure.RTreeBounds{MinX: minLongitude, MinY: bounds.MinLatitude, MaxX: maxLongitude, MaxY: bounds.MaxLatitude})
	}
	var ids []uint64
	var err error
	if bounds.MinLongitude <= bounds.MaxLongitude {
		ids, err = search(bounds.MinLongitude, bounds.MaxLongitude)
	} else {
		var left, right []uint64
		left, err = search(bounds.MinLongitude, 180)
		if err == nil {
			right, err = search(-180, bounds.MaxLongitude)
		}
		ids = append(left, right...)
	}
	if err != nil {
		return nil, err
	}
	positions := make([]int, 0, len(ids)+len(index.fallback))
	for _, id := range ids {
		if id == 0 || id > uint64(rowCount) {
			continue
		}
		positions = append(positions, int(id-1))
	}
	for position := range index.fallback {
		if position >= 0 && position < rowCount {
			positions = append(positions, position)
		}
	}
	sort.Ints(positions)
	if len(positions) > 1 {
		unique := positions[:1]
		for _, position := range positions[1:] {
			if position != unique[len(unique)-1] {
				unique = append(unique, position)
			}
		}
		positions = unique
	}
	return positions, nil
}
