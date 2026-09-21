package hatSql

import (
	"context"
	"testing"
)

func BenchmarkC231SQLWorkloadGroupAdmission(b *testing.B) {
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{
		Default: SQLClusterAdmissionPolicy{
			Serving: SQLClusterAdmissionPool{
				CPUUnits:    64,
				MemoryBytes: 1 << 20,
				MaxRunning:  64,
			},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	query := "FROM VALUES ('a', 1), ('b', 2) AS src(group_id, value) SELECT src.group_id, src.value ORDER BY src.group_id"
	cases := []struct {
		name    string
		options SQLQueryOptions
	}{
		{name: "default", options: SQLQueryOptions{}},
		{
			name: "workload-group",
			options: SQLQueryOptions{
				ClusterAdmission: admission,
				ClusterAdmissionRequest: SQLClusterAdmissionRequest{
					Cluster:     "c231-benchmark",
					Class:       SQLClusterWorkServing,
					CPUUnits:    1,
					MemoryBytes: 128,
				},
			},
		},
	}
	for _, test := range cases {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if _, err := ExecuteSQLQueryContext(context.Background(), query, nil, test.options); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
