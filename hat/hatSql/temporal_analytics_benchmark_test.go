package hatSql

import (
	"strconv"
	"testing"
	"time"
)

const temporalAnalyticsBenchmarkVersions = 10_000

func BenchmarkTemporalTableChronologicalUpsert(b *testing.B) {
	base := time.Unix(0, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		table := NewTemporalTable()
		for version := 0; version < temporalAnalyticsBenchmarkVersions; version++ {
			table.Upsert("account-1", base.Add(time.Duration(version)*time.Second), Row{"value": version})
		}
	}
}

func BenchmarkTemporalTableAsOfLatest(b *testing.B) {
	base := time.Unix(0, 0).UTC()
	table := NewTemporalTable()
	for version := 0; version < temporalAnalyticsBenchmarkVersions; version++ {
		table.Upsert("account-"+strconv.Itoa(version%8), base.Add(time.Duration(version)*time.Second), Row{"value": version})
	}
	at := base.Add(time.Duration(temporalAnalyticsBenchmarkVersions-1) * time.Second)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		row, found := table.AsOf("account-7", at)
		if !found || row["value"] == nil {
			b.Fatalf("AsOf() = %#v, %t", row, found)
		}
	}
}
