package hatReplication

import (
	"fmt"
	"testing"
)

func BenchmarkClusterWriteCommitParticipant(b *testing.B) {
	participant, err := NewClusterWriteCommitParticipant(ClusterWriteCommitParticipantOptions{MaxRecords: 1024})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		proposal := ClusterWriteCommitProposal{TransactionID: fmt.Sprintf("tx-%04d", index), Sequence: uint64(index + 1), FenceToken: 7}
		if _, err := participant.Prepare(proposal); err != nil {
			b.Fatal(err)
		}
		if _, err := participant.Commit(proposal); err != nil {
			b.Fatal(err)
		}
	}
	snapshot, err := participant.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.Run("marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(snapshot)))
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := participant.MarshalSnapshot(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("restore", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(snapshot)))
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := participant.RestoreSnapshot(snapshot); err != nil {
				b.Fatal(err)
			}
		}
	})
}
