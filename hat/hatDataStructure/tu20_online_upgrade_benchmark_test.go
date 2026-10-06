package hatDataStructure

import "testing"

var (
	tu20UpgradeBenchmarkTuple      VersionedTuple
	tu20UpgradeBenchmarkCheckpoint TupleUpgradeCheckpoint
	tu20UpgradeBenchmarkWire       []byte
)

func BenchmarkTU20UpgradeReadOld(b *testing.B) {
	previous, next, converter := tu20UpgradeFormats(b)
	upgrade, err := NewOnlineTupleUpgrade("users-v1-v2", previous, next, 1, converter)
	if err != nil {
		b.Fatal(err)
	}
	row := tu20UpgradeOldRow(b, previous, 1, "alice")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		converted, err := upgrade.Read(row)
		if err != nil {
			b.Fatal(err)
		}
		tu20UpgradeBenchmarkTuple = converted
	}
}

func BenchmarkTU20UpgradeReadNext(b *testing.B) {
	previous, next, converter := tu20UpgradeFormats(b)
	upgrade, err := NewOnlineTupleUpgrade("users-v1-v2", previous, next, 1, converter)
	if err != nil {
		b.Fatal(err)
	}
	row := tu20UpgradeNewRow(b, next, 1, "alice")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		converted, err := upgrade.Read(row)
		if err != nil {
			b.Fatal(err)
		}
		tu20UpgradeBenchmarkTuple = converted
	}
}

func BenchmarkTU20UpgradeCheckpointMarshal(b *testing.B) {
	checkpoint := TupleUpgradeCheckpoint{
		ID:              "users-v1-v2",
		PreviousVersion: 1,
		NextVersion:     2,
		TotalRows:       100000,
		NextRow:         50000,
		MigratedRows:    49999,
		Generation:      12,
		Phase:           TupleUpgradePhaseActive,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wire, err := MarshalTupleUpgradeCheckpoint(checkpoint)
		if err != nil {
			b.Fatal(err)
		}
		tu20UpgradeBenchmarkWire = wire
	}
	b.ReportMetric(float64(len(tu20UpgradeBenchmarkWire)), "wire_bytes/op")
}

func BenchmarkTU20UpgradeCheckpointUnmarshal(b *testing.B) {
	checkpoint := TupleUpgradeCheckpoint{
		ID:              "users-v1-v2",
		PreviousVersion: 1,
		NextVersion:     2,
		TotalRows:       100000,
		NextRow:         50000,
		MigratedRows:    49999,
		Generation:      12,
		Phase:           TupleUpgradePhaseActive,
	}
	wire, err := MarshalTupleUpgradeCheckpoint(checkpoint)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded, err := UnmarshalTupleUpgradeCheckpoint(wire)
		if err != nil {
			b.Fatal(err)
		}
		tu20UpgradeBenchmarkCheckpoint = decoded
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes/op")
}
