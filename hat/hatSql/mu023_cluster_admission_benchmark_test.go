package hatSql

import (
	"context"
	"testing"
)

var mu023ClusterAdmissionSink uint64

//go:noinline
func mu023AdmittedExecution(context.Context) error {
	mu023ClusterAdmissionSink++
	return nil
}

func BenchmarkMU023AfterClusterAdmissionExecute(b *testing.B) {
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{})
	if err != nil {
		b.Fatal(err)
	}
	request := SQLClusterAdmissionRequest{
		Cluster:  "analytics",
		Class:    SQLClusterWorkServing,
		CPUUnits: 1,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := admission.Execute(context.Background(), request, mu023AdmittedExecution); err != nil {
			b.Fatal(err)
		}
	}
}
