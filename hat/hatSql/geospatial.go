package hatSql

import (
	"fmt"
	"math"
	"sort"
	"sync"
)

const geoEarthRadiusMeters = 6_371_008.8

// GeoPoint is a WGS84 latitude and longitude in decimal degrees.
type GeoPoint struct {
	Latitude  float64
	Longitude float64
}

// GeoBoundingBox includes its edges. A minimum longitude greater than the
// maximum longitude denotes a box crossing the international date line.
type GeoBoundingBox struct {
	MinLatitude  float64
	MaxLatitude  float64
	MinLongitude float64
	MaxLongitude float64
}

type geoCell struct{ latitude, longitude int }

// GeoIndex is an in-memory grid index for point predicates. cellDegrees trades
// a small amount of candidate filtering work for lower bucket-map overhead.
type GeoIndex struct {
	mu          sync.RWMutex
	cellDegrees float64
	latitudeN   int
	longitudeN  int
	points      map[string]GeoPoint
	buckets     map[geoCell]map[string]struct{}
}

// NewGeoIndex builds a point index with the requested grid-cell width.
func NewGeoIndex(cellDegrees float64) (*GeoIndex, error) {
	if math.IsNaN(cellDegrees) || math.IsInf(cellDegrees, 0) || cellDegrees <= 0 || cellDegrees > 360 {
		return nil, fmt.Errorf("geo index cell degrees must be in (0, 360]")
	}
	return &GeoIndex{
		cellDegrees: cellDegrees,
		latitudeN:   int(math.Ceil(180 / cellDegrees)),
		longitudeN:  int(math.Ceil(360 / cellDegrees)),
		points:      make(map[string]GeoPoint),
		buckets:     make(map[geoCell]map[string]struct{}),
	}, nil
}

// GeoDistanceMeters returns the great-circle distance between two valid points.
func GeoDistanceMeters(left, right GeoPoint) (float64, error) {
	if err := left.validate(); err != nil {
		return 0, err
	}
	if err := right.validate(); err != nil {
		return 0, err
	}
	latitude1 := left.Latitude * math.Pi / 180
	latitude2 := right.Latitude * math.Pi / 180
	deltaLatitude := latitude2 - latitude1
	deltaLongitude := (right.Longitude - left.Longitude) * math.Pi / 180
	a := math.Sin(deltaLatitude/2)*math.Sin(deltaLatitude/2) + math.Cos(latitude1)*math.Cos(latitude2)*math.Sin(deltaLongitude/2)*math.Sin(deltaLongitude/2)
	return geoEarthRadiusMeters * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a)), nil
}

// Upsert adds or replaces a point under id.
func (index *GeoIndex) Upsert(id string, point GeoPoint) error {
	if index == nil {
		return fmt.Errorf("geo index is nil")
	}
	if id == "" {
		return fmt.Errorf("geo index id cannot be empty")
	}
	if err := point.validate(); err != nil {
		return err
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	if old, ok := index.points[id]; ok {
		index.removeLocked(id, old)
	}
	index.points[id] = point
	cell := index.cellFor(point)
	if index.buckets[cell] == nil {
		index.buckets[cell] = make(map[string]struct{})
	}
	index.buckets[cell][id] = struct{}{}
	return nil
}

// Delete removes id and reports whether it was present.
func (index *GeoIndex) Delete(id string) bool {
	if index == nil || id == "" {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	point, ok := index.points[id]
	if !ok {
		return false
	}
	index.removeLocked(id, point)
	delete(index.points, id)
	return true
}

// WithinBox returns point IDs inside bounds in ascending order.
func (index *GeoIndex) WithinBox(bounds GeoBoundingBox) ([]string, error) {
	if index == nil {
		return nil, fmt.Errorf("geo index is nil")
	}
	if err := bounds.validate(); err != nil {
		return nil, err
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	candidates := make(map[string]struct{})
	minimumLatitude := index.latitudeCell(bounds.MinLatitude)
	maximumLatitude := index.latitudeCell(bounds.MaxLatitude)
	minimumLongitude := index.longitudeCell(bounds.MinLongitude)
	maximumLongitude := index.longitudeCell(bounds.MaxLongitude)
	if bounds.MinLongitude <= bounds.MaxLongitude {
		index.addCellsLocked(candidates, minimumLatitude, maximumLatitude, minimumLongitude, maximumLongitude)
	} else {
		index.addCellsLocked(candidates, minimumLatitude, maximumLatitude, minimumLongitude, index.longitudeN-1)
		index.addCellsLocked(candidates, minimumLatitude, maximumLatitude, 0, maximumLongitude)
	}
	ids := make([]string, 0, len(candidates))
	for id := range candidates {
		if bounds.contains(index.points[id]) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// WithinRadius returns point IDs whose exact great-circle distance is no more
// than radiusMeters, in ascending order.
func (index *GeoIndex) WithinRadius(center GeoPoint, radiusMeters float64) ([]string, error) {
	if index == nil {
		return nil, fmt.Errorf("geo index is nil")
	}
	if err := center.validate(); err != nil {
		return nil, err
	}
	if math.IsNaN(radiusMeters) || math.IsInf(radiusMeters, 0) || radiusMeters < 0 {
		return nil, fmt.Errorf("geo radius meters must be finite and non-negative")
	}
	bounds := geoRadiusBounds(center, radiusMeters)
	candidates, err := index.WithinBox(bounds)
	if err != nil {
		return nil, err
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	ids := candidates[:0]
	for _, id := range candidates {
		distance, err := GeoDistanceMeters(center, index.points[id])
		if err != nil {
			return nil, err
		}
		if distance <= radiusMeters {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (point GeoPoint) validate() error {
	if math.IsNaN(point.Latitude) || math.IsInf(point.Latitude, 0) || point.Latitude < -90 || point.Latitude > 90 {
		return fmt.Errorf("geo latitude must be finite and in [-90, 90]")
	}
	if math.IsNaN(point.Longitude) || math.IsInf(point.Longitude, 0) || point.Longitude < -180 || point.Longitude > 180 {
		return fmt.Errorf("geo longitude must be finite and in [-180, 180]")
	}
	return nil
}

func (bounds GeoBoundingBox) validate() error {
	if err := (GeoPoint{Latitude: bounds.MinLatitude, Longitude: bounds.MinLongitude}).validate(); err != nil {
		return err
	}
	if err := (GeoPoint{Latitude: bounds.MaxLatitude, Longitude: bounds.MaxLongitude}).validate(); err != nil {
		return err
	}
	if bounds.MinLatitude > bounds.MaxLatitude {
		return fmt.Errorf("geo bounding box minimum latitude exceeds maximum latitude")
	}
	return nil
}

func (bounds GeoBoundingBox) contains(point GeoPoint) bool {
	if point.Latitude < bounds.MinLatitude || point.Latitude > bounds.MaxLatitude {
		return false
	}
	if bounds.MinLongitude <= bounds.MaxLongitude {
		return point.Longitude >= bounds.MinLongitude && point.Longitude <= bounds.MaxLongitude
	}
	return point.Longitude >= bounds.MinLongitude || point.Longitude <= bounds.MaxLongitude
}

func (index *GeoIndex) removeLocked(id string, point GeoPoint) {
	cell := index.cellFor(point)
	delete(index.buckets[cell], id)
	if len(index.buckets[cell]) == 0 {
		delete(index.buckets, cell)
	}
}

func (index *GeoIndex) cellFor(point GeoPoint) geoCell {
	return geoCell{latitude: index.latitudeCell(point.Latitude), longitude: index.longitudeCell(point.Longitude)}
}

func (index *GeoIndex) latitudeCell(latitude float64) int {
	return geoCellCoordinate((latitude+90)/index.cellDegrees, index.latitudeN)
}

func (index *GeoIndex) longitudeCell(longitude float64) int {
	return geoCellCoordinate((longitude+180)/index.cellDegrees, index.longitudeN)
}

func geoCellCoordinate(value float64, count int) int {
	coordinate := int(math.Floor(value))
	if coordinate < 0 {
		return 0
	}
	if coordinate >= count {
		return count - 1
	}
	return coordinate
}

func (index *GeoIndex) addCellsLocked(candidates map[string]struct{}, minimumLatitude, maximumLatitude, minimumLongitude, maximumLongitude int) {
	for latitude := minimumLatitude; latitude <= maximumLatitude; latitude++ {
		for longitude := minimumLongitude; longitude <= maximumLongitude; longitude++ {
			for id := range index.buckets[geoCell{latitude: latitude, longitude: longitude}] {
				candidates[id] = struct{}{}
			}
		}
	}
}

func geoRadiusBounds(center GeoPoint, radiusMeters float64) GeoBoundingBox {
	angularDistance := math.Min(math.Pi, radiusMeters/geoEarthRadiusMeters)
	latitudeDelta := angularDistance * 180 / math.Pi
	minimumLatitude := math.Max(-90, center.Latitude-latitudeDelta)
	maximumLatitude := math.Min(90, center.Latitude+latitudeDelta)
	if minimumLatitude == -90 || maximumLatitude == 90 {
		return GeoBoundingBox{MinLatitude: minimumLatitude, MaxLatitude: maximumLatitude, MinLongitude: -180, MaxLongitude: 180}
	}
	longitudeDelta := math.Asin(math.Min(1, math.Sin(angularDistance)/math.Cos(center.Latitude*math.Pi/180))) * 180 / math.Pi
	minimumLongitude := center.Longitude - longitudeDelta
	maximumLongitude := center.Longitude + longitudeDelta
	if minimumLongitude < -180 {
		minimumLongitude += 360
	}
	if maximumLongitude > 180 {
		maximumLongitude -= 360
	}
	return GeoBoundingBox{MinLatitude: minimumLatitude, MaxLatitude: maximumLatitude, MinLongitude: minimumLongitude, MaxLongitude: maximumLongitude}
}
