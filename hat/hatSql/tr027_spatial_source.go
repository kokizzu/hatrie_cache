package hatSql

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"hatrie_cache/hat/hatDataStructure"
)

// SQLGeoPredicateKind identifies the bounded spatial predicate understood by
// the SQL spatial index hook.
type SQLGeoPredicateKind uint8

const (
	SQLGeoPredicateWithinBox SQLGeoPredicateKind = iota + 1
	SQLGeoPredicateWithinRadius
)

// SQLGeoPredicate is a validated, literal spatial predicate extracted from a
// GEO_WITHIN_BOX or GEO_WITHIN_RADIUS SQL expression.
type SQLGeoPredicate struct {
	Kind           SQLGeoPredicateKind
	LatitudeField  string
	LongitudeField string
	Bounds         GeoBoundingBox
	Center         GeoPoint
	RadiusMeters   float64
}

// RTreeSpatialSourceOptions configures one row source backed by an R-tree.
// Rows whose coordinate fields are NULL or not indexable remain candidates so
// normal SQL NULL and evaluation-error behavior is preserved.
type RTreeSpatialSourceOptions struct {
	SourceName     string
	Name           string
	LatitudeField  string
	LongitudeField string
	MaxEntries     int
}

// RTreeSpatialSourceStats is a cumulative source-activity snapshot.
type RTreeSpatialSourceStats struct {
	FullScans      uint64
	SpatialQueries uint64
	CandidateRows  uint64
}

// RTreeSpatialSource is a mutable SQL source with a point R-tree. It is
// useful for bounded GEO_WITHIN_BOX and GEO_WITHIN_RADIUS queries while
// retaining the ordinary SourceResolver contract for all other SQL.
type RTreeSpatialSource struct {
	mu             sync.RWMutex
	sourceName     string
	name           string
	latitudeField  string
	longitudeField string
	tree           *hatDataStructure.RTree
	rows           map[uint64]Row
	keys           map[string]uint64
	order          []uint64
	fallback       map[uint64]struct{}
	nextID         uint64
	fullScans      uint64
	spatialQueries uint64
	candidateRows  uint64
}

// NewRTreeSpatialSource creates an empty R-tree-backed SQL source.
func NewRTreeSpatialSource(options RTreeSpatialSourceOptions) (*RTreeSpatialSource, error) {
	sourceName := strings.ToUpper(strings.TrimSpace(options.SourceName))
	if sourceName == "" {
		sourceName = "CACHE"
	}
	name := strings.TrimSpace(options.Name)
	latitudeField := strings.TrimSpace(options.LatitudeField)
	longitudeField := strings.TrimSpace(options.LongitudeField)
	if name == "" {
		return nil, fmt.Errorf("rtree spatial source name is required")
	}
	if latitudeField == "" || longitudeField == "" || latitudeField == longitudeField {
		return nil, fmt.Errorf("rtree spatial source latitude and longitude fields must be distinct")
	}
	tree, err := hatDataStructure.NewRTree(options.MaxEntries)
	if err != nil {
		return nil, err
	}
	return &RTreeSpatialSource{
		sourceName:     sourceName,
		name:           name,
		latitudeField:  latitudeField,
		longitudeField: longitudeField,
		tree:           tree,
		rows:           make(map[uint64]Row),
		keys:           make(map[string]uint64),
		fallback:       make(map[uint64]struct{}),
		nextID:         1,
	}, nil
}

// Upsert inserts or replaces one row. key is the stable row identity used by
// the R-tree and is not added to the returned row automatically.
func (source *RTreeSpatialSource) Upsert(key string, row Row) error {
	if source == nil {
		return fmt.Errorf("rtree spatial source is nil")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("rtree spatial source row key is required")
	}
	point, indexed := source.pointForRow(row)
	source.mu.Lock()
	defer source.mu.Unlock()
	id, exists := source.keys[key]
	if !exists {
		if source.nextID == 0 {
			return fmt.Errorf("rtree spatial source row id space exhausted")
		}
		id = source.nextID
		source.nextID++
		source.keys[key] = id
		source.order = append(source.order, id)
	}
	if exists {
		source.tree.Delete(id)
		delete(source.fallback, id)
	}
	if indexed {
		if err := source.tree.Upsert(id, hatDataStructure.RTreeBounds{MinX: point.Longitude, MinY: point.Latitude, MaxX: point.Longitude, MaxY: point.Latitude}); err != nil {
			return err
		}
	} else {
		source.fallback[id] = struct{}{}
	}
	source.rows[id] = cloneSQLRow(row)
	return nil
}

// Delete removes a row and reports whether it existed.
func (source *RTreeSpatialSource) Delete(key string) bool {
	if source == nil {
		return false
	}
	key = strings.TrimSpace(key)
	source.mu.Lock()
	defer source.mu.Unlock()
	id, ok := source.keys[key]
	if !ok {
		return false
	}
	source.tree.Delete(id)
	delete(source.keys, key)
	delete(source.rows, id)
	delete(source.fallback, id)
	return true
}

// Stats returns cumulative source activity without exposing mutable internals.
func (source *RTreeSpatialSource) Stats() RTreeSpatialSourceStats {
	if source == nil {
		return RTreeSpatialSourceStats{}
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	return RTreeSpatialSourceStats{
		FullScans:      source.fullScans,
		SpatialQueries: source.spatialQueries,
		CandidateRows:  source.candidateRows,
	}
}

// ResolveSQLSource implements the ordinary source contract for non-spatial
// queries and for fallback when no supported spatial predicate is present.
func (source *RTreeSpatialSource) ResolveSQLSource(name, key string) ([]Row, error) {
	if source == nil || !source.matches(name, key) {
		return nil, nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	source.fullScans++
	rows := make([]Row, 0, len(source.rows))
	for _, id := range source.order {
		row, ok := source.rows[id]
		if ok {
			rows = append(rows, cloneSQLRow(row))
		}
	}
	return rows, nil
}

// ResolveSQLGeoSource returns R-tree candidates for a matching source and
// coordinate pair. The SQL executor evaluates the complete predicate after
// this call, including the exact great-circle radius check.
func (source *RTreeSpatialSource) ResolveSQLGeoSource(name, key string, predicate SQLGeoPredicate) ([]Row, bool, error) {
	if source == nil || !source.matches(name, key) {
		return nil, false, nil
	}
	if predicate.LatitudeField != source.latitudeField || predicate.LongitudeField != source.longitudeField {
		return nil, false, nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	var ids []uint64
	switch predicate.Kind {
	case SQLGeoPredicateWithinBox:
		if err := predicate.Bounds.validate(); err != nil {
			return nil, true, err
		}
		ids, _ = source.searchBoundsLocked(predicate.Bounds)
	case SQLGeoPredicateWithinRadius:
		if err := predicate.Center.validate(); err != nil {
			return nil, true, err
		}
		if math.IsNaN(predicate.RadiusMeters) || math.IsInf(predicate.RadiusMeters, 0) || predicate.RadiusMeters < 0 {
			return nil, true, fmt.Errorf("geo radius meters must be finite and non-negative")
		}
		ids, _ = source.searchBoundsLocked(geoRadiusBounds(predicate.Center, predicate.RadiusMeters))
	default:
		return nil, false, nil
	}
	for id := range source.fallback {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
	if len(ids) > 1 {
		unique := ids[:1]
		for _, id := range ids[1:] {
			if id != unique[len(unique)-1] {
				unique = append(unique, id)
			}
		}
		ids = unique
	}
	rows := make([]Row, 0, len(ids))
	for _, id := range ids {
		if row, ok := source.rows[id]; ok {
			rows = append(rows, cloneSQLRow(row))
		}
	}
	source.spatialQueries++
	source.candidateRows += uint64(len(rows))
	return rows, true, nil
}

func (source *RTreeSpatialSource) matches(name, key string) bool {
	return strings.EqualFold(strings.TrimSpace(name), source.sourceName) && key == source.name
}

func (source *RTreeSpatialSource) pointForRow(row Row) (GeoPoint, bool) {
	latitude, latitudeOK := sqlNumber(row[source.latitudeField])
	longitude, longitudeOK := sqlNumber(row[source.longitudeField])
	point := GeoPoint{Latitude: latitude, Longitude: longitude}
	if !latitudeOK || !longitudeOK || point.validate() != nil {
		return GeoPoint{}, false
	}
	return point, true
}

func (source *RTreeSpatialSource) searchBoundsLocked(bounds GeoBoundingBox) ([]uint64, error) {
	search := func(minLongitude, maxLongitude float64) ([]uint64, error) {
		return source.tree.Search(hatDataStructure.RTreeBounds{
			MinX: minLongitude,
			MinY: bounds.MinLatitude,
			MaxX: maxLongitude,
			MaxY: bounds.MaxLatitude,
		})
	}
	if bounds.MinLongitude <= bounds.MaxLongitude {
		ids, err := search(bounds.MinLongitude, bounds.MaxLongitude)
		return ids, err
	}
	left, err := search(bounds.MinLongitude, 180)
	if err != nil {
		return nil, err
	}
	right, err := search(-180, bounds.MaxLongitude)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}

func cloneSQLRow(row Row) Row {
	if row == nil {
		return nil
	}
	clone := make(Row, len(row))
	for key, value := range row {
		clone[key] = value
	}
	return clone
}

func resolveSQLGeoIndexedSource(source sqlSource, condition sqlExpr, resolver SQLSourceResolver, hint SQLIndexHint) ([]SQLRow, bool, error) {
	predicate, ok := sqlGeoIndexedPredicate(source, condition)
	if !ok || !hint.allowsField(source, predicate.LatitudeField) || !hint.allowsField(source, predicate.LongitudeField) {
		return nil, false, nil
	}
	indexed, ok := resolver.(GeoIndexedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return indexed.ResolveSQLGeoSource(source.kind, source.key, predicate)
}

func sqlGeoIndexedPredicate(source sqlSource, condition sqlExpr) (SQLGeoPredicate, bool) {
	if condition.kind != "func" || len(condition.args) == 0 {
		return SQLGeoPredicate{}, false
	}
	if len(condition.args) < 2 || condition.args[0].kind != "field" || condition.args[1].kind != "field" {
		return SQLGeoPredicate{}, false
	}
	latitude, longitude := condition.args[0], condition.args[1]
	if (latitude.qualifier != "" && !strings.EqualFold(latitude.qualifier, source.alias)) || (longitude.qualifier != "" && !strings.EqualFold(longitude.qualifier, source.alias)) {
		return SQLGeoPredicate{}, false
	}
	if latitude.name == "" || longitude.name == "" {
		return SQLGeoPredicate{}, false
	}
	number := func(expr sqlExpr) (float64, bool) {
		if expr.kind != "literal" || expr.value == nil {
			return 0, false
		}
		value, ok := sqlNumber(expr.value)
		return value, ok && isFiniteSQLNumber(value)
	}
	switch {
	case strings.EqualFold(condition.name, "GEO_WITHIN_BOX") && len(condition.args) == 6:
		minLatitude, ok1 := number(condition.args[2])
		maxLatitude, ok2 := number(condition.args[3])
		minLongitude, ok3 := number(condition.args[4])
		maxLongitude, ok4 := number(condition.args[5])
		if !ok1 || !ok2 || !ok3 || !ok4 {
			return SQLGeoPredicate{}, false
		}
		bounds := GeoBoundingBox{MinLatitude: minLatitude, MaxLatitude: maxLatitude, MinLongitude: minLongitude, MaxLongitude: maxLongitude}
		if bounds.validate() != nil {
			return SQLGeoPredicate{}, false
		}
		return SQLGeoPredicate{Kind: SQLGeoPredicateWithinBox, LatitudeField: latitude.name, LongitudeField: longitude.name, Bounds: bounds}, true
	case strings.EqualFold(condition.name, "GEO_WITHIN_RADIUS") && len(condition.args) == 5:
		centerLatitude, ok1 := number(condition.args[2])
		centerLongitude, ok2 := number(condition.args[3])
		radius, ok3 := number(condition.args[4])
		center := GeoPoint{Latitude: centerLatitude, Longitude: centerLongitude}
		if !ok1 || !ok2 || !ok3 || center.validate() != nil || radius < 0 {
			return SQLGeoPredicate{}, false
		}
		return SQLGeoPredicate{Kind: SQLGeoPredicateWithinRadius, LatitudeField: latitude.name, LongitudeField: longitude.name, Center: center, RadiusMeters: radius}, true
	default:
		return SQLGeoPredicate{}, false
	}
}
