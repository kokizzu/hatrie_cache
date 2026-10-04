package hatSql_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestResultCacheAdmissionPublicAPI(t *testing.T) {
	cache, err := hatSql.NewSQLResultCacheWithAdmission(2, hatSql.ResultCacheAdmissionPolicy{
		MinExecutionDuration: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSQLResultCacheWithAdmission() error = %v", err)
	}
	if stats := cache.AdmissionStats(); stats.Admitted != 0 || stats.Rejected != 0 {
		t.Fatalf("initial admission stats = %#v", stats)
	}
}
