package hatSql

import (
	"fmt"
	"math"
	"strings"
)

func evalSQLGeoFunction(expr sqlExpr, group []sqlExecRow, row sqlExecRow) interface{} {
	name := strings.ToUpper(expr.name)
	argumentCount := 0
	switch name {
	case "GEO_DISTANCE", "GEO_DISTANCE_METERS":
		argumentCount = 4
	case "GEO_WITHIN_RADIUS":
		argumentCount = 5
	case "GEO_WITHIN_BOX":
		argumentCount = 6
	default:
		return sqlEvalError{err: fmt.Errorf("unknown SQL spatial function %q", name), token: expr.token}
	}
	if len(expr.args) != argumentCount {
		return sqlEvalError{err: fmt.Errorf("%s expects exactly %d arguments", name, argumentCount), token: expr.token}
	}
	arguments := make([]float64, argumentCount)
	for index, argument := range expr.args {
		value := evalSQLExpr(argument, group, row)
		if err := sqlExpressionError(value); err != nil {
			return sqlEvaluationFailure(err)
		}
		if value == nil {
			return nil
		}
		number, ok := sqlNumber(value)
		if !ok {
			return sqlEvalError{err: fmt.Errorf("%s expects numeric arguments", name), token: expr.token}
		}
		arguments[index] = number
	}
	switch name {
	case "GEO_DISTANCE", "GEO_DISTANCE_METERS":
		distance, err := GeoDistanceMeters(
			GeoPoint{Latitude: arguments[0], Longitude: arguments[1]},
			GeoPoint{Latitude: arguments[2], Longitude: arguments[3]},
		)
		if err != nil {
			return sqlEvalError{err: err, token: expr.token}
		}
		return distance
	case "GEO_WITHIN_RADIUS":
		if arguments[4] < 0 || !isFiniteSQLNumber(arguments[4]) {
			return sqlEvalError{err: fmt.Errorf("GEO_WITHIN_RADIUS radius must be finite and non-negative"), token: expr.token}
		}
		distance, err := GeoDistanceMeters(
			GeoPoint{Latitude: arguments[0], Longitude: arguments[1]},
			GeoPoint{Latitude: arguments[2], Longitude: arguments[3]},
		)
		if err != nil {
			return sqlEvalError{err: err, token: expr.token}
		}
		return distance <= arguments[4]
	case "GEO_WITHIN_BOX":
		bounds := GeoBoundingBox{
			MinLatitude:  arguments[2],
			MaxLatitude:  arguments[3],
			MinLongitude: arguments[4],
			MaxLongitude: arguments[5],
		}
		point := GeoPoint{Latitude: arguments[0], Longitude: arguments[1]}
		if err := point.validate(); err != nil {
			return sqlEvalError{err: err, token: expr.token}
		}
		if err := bounds.validate(); err != nil {
			return sqlEvalError{err: err, token: expr.token}
		}
		return bounds.contains(point)
	}
	return nil
}

func isFiniteSQLNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
