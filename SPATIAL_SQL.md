# SQL Spatial Predicates

`hatSql` exposes the existing WGS84 geospatial primitives as SQL built-ins:

```sql
GEO_DISTANCE_METERS(latitude1, longitude1, latitude2, longitude2)
GEO_WITHIN_RADIUS(latitude, longitude, center_latitude, center_longitude, radius_meters)
GEO_WITHIN_BOX(latitude, longitude, min_latitude, max_latitude, min_longitude, max_longitude)
```

`GEO_DISTANCE` is an alias for `GEO_DISTANCE_METERS`. Coordinates use decimal
degrees, distances use meters, and a box with `min_longitude > max_longitude`
crosses the international date line.

```sql
FROM CACHE('locations') AS location
WHERE GEO_WITHIN_RADIUS(location.latitude, location.longitude, 1.3521, 103.8198, 20000)
SELECT location.name
```

Any NULL argument produces SQL NULL, so it is not selected by a `WHERE`
predicate. Invalid coordinates, inverted latitude bounds, or a negative or
non-finite radius return a query error. The functions use the same evaluator
in materialized and streamed query paths; existing non-spatial queries are
unchanged.
