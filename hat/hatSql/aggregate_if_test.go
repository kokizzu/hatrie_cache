package hatSql

import (
	"reflect"
	"testing"
)

func TestSQLClickHouseAggregateIfVariants(t *testing.T) {
	result, err := ExecuteSQLQuery(`
FROM VALUES
  (1, true),
  (3, true),
  (9, false),
  (NULL, true)
AS values(amount, active)
SELECT
  COUNT_IF(active) AS count_true,
  SUM_IF(amount, active) AS sum_true,
  AVG_IF(amount, active) AS avg_true,
  MIN_IF(amount, active) AS min_true,
  MAX_IF(amount, active) AS max_true,
  ARGMAX_IF(amount, amount, active) AS argmax_true,
  ARGMIN_IF(amount, amount, active) AS argmin_true`, nil)
	if err != nil {
		t.Fatalf("aggregate If query error = %v", err)
	}
	want := []SQLRow{{
		"count_true":  int64(3),
		"sum_true":    float64(4),
		"avg_true":    float64(2),
		"min_true":    float64(1),
		"max_true":    float64(3),
		"argmax_true": int64(3),
		"argmin_true": int64(1),
	}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("aggregate If rows = %#v, want %#v", result.Rows, want)
	}

	alias, err := ExecuteSQLQuery(`
FROM VALUES (1, true), (2, false) AS values(amount, active)
SELECT COUNTIF(active) AS count_true`, nil)
	if err != nil {
		t.Fatalf("compact aggregate If query error = %v", err)
	}
	if !reflect.DeepEqual(alias.Rows, []SQLRow{{"count_true": int64(1)}}) {
		t.Fatalf("compact aggregate If rows = %#v", alias.Rows)
	}
}

func TestSQLClickHouseAggregateIfGroupedHaving(t *testing.T) {
	result, err := ExecuteSQLQuery(`
FROM VALUES
  ('a', 1, true),
  ('a', 3, false),
  ('b', 5, true),
  ('b', 7, false),
  ('c', 9, false)
AS values(bucket, amount, active)
SELECT
  bucket,
  COUNT_IF(active) AS count_true,
  SUM_IF(amount, active) AS sum_true
GROUP BY bucket
HAVING COUNT_IF(active) > 0
ORDER BY bucket`, nil)
	if err != nil {
		t.Fatalf("grouped aggregate If query error = %v", err)
	}
	want := []SQLRow{
		{"bucket": "a", "count_true": int64(1), "sum_true": float64(1)},
		{"bucket": "b", "count_true": int64(1), "sum_true": float64(5)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("grouped aggregate If rows = %#v, want %#v", result.Rows, want)
	}
}
