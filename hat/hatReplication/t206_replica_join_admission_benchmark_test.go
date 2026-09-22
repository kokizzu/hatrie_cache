package hatReplication

import (
	"fmt"
	"testing"
)

func BenchmarkT206PrepareAndAbort(b *testing.B) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{MaxMembers: 1})
	if err != nil {
		b.Fatalf("NewReplicaJoinAdmission() error = %v", err)
	}
	request := validReplicaJoinRequest("joiner", "https://joiner", 1)
	b.ReportAllocs()
	for range b.N {
		decision, err := admission.Prepare(request)
		if err != nil {
			b.Fatal(err)
		}
		if err := admission.Abort(decision); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT206Snapshot128Members(b *testing.B) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{MaxMembers: 128})
	if err != nil {
		b.Fatalf("NewReplicaJoinAdmission() error = %v", err)
	}
	for index := 0; index < 128; index++ {
		request := validReplicaJoinRequest(fmt.Sprintf("joiner-%03d", index), fmt.Sprintf("https://joiner-%03d", index), uint64(index+1))
		decision, err := admission.Prepare(request)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := admission.Commit(decision, activeReplicaJoinState(decision)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for range b.N {
		snapshot := admission.Snapshot()
		if len(snapshot.Members) != 128 {
			b.Fatal("snapshot lost members")
		}
	}
}
