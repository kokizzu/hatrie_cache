package hatSchema

import (
	"context"
	"strconv"
	"testing"
)

var onlineSpaceBenchmarkSink []Row

func BenchmarkOnlineSpaceSynchronousBaseline(b *testing.B) {
	rows := benchmarkOnlineSpaceRows(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		converted := make([]Row, len(rows))
		for rowIndex, row := range rows {
			converted[rowIndex] = benchmarkOnlineSpaceConvert(row)
		}
		onlineSpaceBenchmarkSink = converted
	}
}

func BenchmarkOnlineSpaceBackground(b *testing.B) {
	rows := benchmarkOnlineSpaceRows(4096)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		b.StopTimer()
		space := benchmarkOnlineSpaceFixture(b, rows)
		b.StartTimer()
		upgrade, err := space.BeginUpgrade(context.Background(), benchmarkOnlineSpaceTarget(), benchmarkOnlineSpaceConvertContext)
		if err != nil {
			b.Fatal(err)
		}
		if err := upgrade.Wait(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		onlineSpaceBenchmarkSink = space.Rows()
	}
}

func benchmarkOnlineSpaceRows(count int) []Row {
	rows := make([]Row, count)
	for index := range rows {
		rows[index] = Row{"id": strconv.Itoa(index), "name": "name-" + strconv.Itoa(index)}
	}
	return rows
}

func benchmarkOnlineSpaceTarget() SpaceDefinition {
	return SpaceDefinition{
		Name:    "users",
		Version: 2,
		Source:  Source{Name: "users", Columns: []Column{{Name: "id", Type: TypeText}, {Name: "name", Type: TypeText}, {Name: "email", Type: TypeText}}},
	}
}

func benchmarkOnlineSpaceFixture(b *testing.B, rows []Row) *OnlineSpace {
	b.Helper()
	initial := SpaceDefinition{
		Name:    "users",
		Version: 1,
		Source:  Source{Name: "users", Columns: []Column{{Name: "id", Type: TypeText}, {Name: "name", Type: TypeText}}},
	}
	space, err := NewOnlineSpace(initial, func(row Row) (string, error) {
		return row["id"].(string), nil
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, row := range rows {
		if err := space.Upsert(row); err != nil {
			b.Fatal(err)
		}
	}
	return space
}

func benchmarkOnlineSpaceConvert(row Row) Row {
	converted := cloneRow(row)
	converted["email"] = converted["id"].(string) + "@example.test"
	return converted
}

func benchmarkOnlineSpaceConvertContext(_ context.Context, row Row) error {
	row["email"] = row["id"].(string) + "@example.test"
	return nil
}
