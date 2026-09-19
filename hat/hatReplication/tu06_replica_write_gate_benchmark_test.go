package hatReplication

import "testing"

var tu06BaselineAdmissionSink bool

var tu06GateAdmissionSink error

func BenchmarkTU06BaselineBooleanAdmission(b *testing.B) {
	readOnly := false
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		tu06BaselineAdmissionSink = !readOnly
	}
}

func BenchmarkTU06ReplicaWriteGateWritable(b *testing.B) {
	gate, err := NewReplicaWriteGate(ReplicaWriteGateOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		tu06GateAdmissionSink = gate.Admit(ReplicaWriteExternal)
	}
}

func BenchmarkTU06ReplicaWriteGateBlocked(b *testing.B) {
	gate, err := NewReplicaWriteGate(ReplicaWriteGateOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := gate.SetReadOnly("benchmark"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		tu06GateAdmissionSink = gate.Admit(ReplicaWriteExternal)
	}
}
