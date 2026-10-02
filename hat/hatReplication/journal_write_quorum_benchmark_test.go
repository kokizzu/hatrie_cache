package hatReplication

import "testing"

func BenchmarkJournalWriteQuorumState(b *testing.B) {
	b.Run("baseline-evaluate", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			decision, err := EvaluateWriteQuorum(3, 2, 2)
			if err != nil || !decision.Satisfied {
				b.Fatalf("EvaluateWriteQuorum() = %#v/%v", decision, err)
			}
		}
	})

	b.Run("state-decision", func(b *testing.B) {
		state, err := NewJournalWriteQuorumState(1, 2, 3)
		if err != nil {
			b.Fatal(err)
		}
		state, err = state.Acknowledge(1)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			decision, err := state.Decision()
			if err != nil || !decision.Satisfied {
				b.Fatalf("Decision() = %#v/%v", decision, err)
			}
		}
	})

	b.Run("state-acknowledge", func(b *testing.B) {
		state, err := NewJournalWriteQuorumState(1, 3, 3)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			state, err = state.Acknowledge(1)
			if err != nil {
				b.Fatal(err)
			}
			state.AcknowledgedMask = 1
		}
	})
}
