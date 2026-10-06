package hatStorage

import (
	"context"
	"testing"
)

type mu033BenchmarkPermit struct{}

func (mu033BenchmarkPermit) Release() error {
	return nil
}

type mu033BenchmarkAdmission struct{}

func (mu033BenchmarkAdmission) BeginCompaction(context.Context, string, uint64) (CompactionBoundaryPermit, error) {
	return mu033BenchmarkPermit{}, nil
}

func BenchmarkMU33CompactionControllerSubmitWithBoundaryAdmission(b *testing.B) {
	controller, err := NewCompactionController(CompactionControllerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	admission := mu033BenchmarkAdmission{}
	request := CompactionRequest{
		Target: "benchmark",
		Run: func(context.Context) error {
			return nil
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		request.Target = "benchmark"
		if _, _, err := controller.SubmitWithBoundaryAdmission(context.Background(), request, admission, "events", uint64(index+1)); err != nil {
			b.Fatal(err)
		}
		if _, err := controller.Run(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
