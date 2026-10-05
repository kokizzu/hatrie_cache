package hatSql

import "testing"

func BenchmarkMU06LegacyBoundedRows(b *testing.B) {
	benchmarkMU06Frame(b, nil)
}

func BenchmarkMU06UnboundedPrecedingRows(b *testing.B) {
	benchmarkMU06Frame(b, &DifferentialWindowFrame{
		Start: DifferentialWindowFrameBound{Kind: DifferentialWindowBoundUnboundedPreceding},
		End:   DifferentialWindowFrameBound{Kind: DifferentialWindowBoundCurrentRow},
	})
}

func benchmarkMU06Frame(b *testing.B, frame *DifferentialWindowFrame) {
	updates := make([]DifferentialRow, 64)
	for index := range updates {
		updates[index] = DifferentialRow{
			Key:  string(rune('a' + index)),
			Time: uint64(index + 1),
			Diff: 1,
			Row:  Row{"value": int64(index)},
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		window, err := NewDifferentialWindow(DifferentialWindowOptions{
			Mode:  DifferentialWindowFrameRows,
			Start: -8,
			End:   0,
		})
		if frame != nil {
			window, err = NewDifferentialWindowWithFrame(DifferentialWindowOptions{Mode: DifferentialWindowFrameRows, Start: -8, End: 0}, *frame)
		}
		if err != nil {
			b.Fatal(err)
		}
		if _, err := window.Apply(updates); err != nil {
			b.Fatal(err)
		}
	}
}
