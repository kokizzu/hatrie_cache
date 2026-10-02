package hatSql_test

import (
	"context"
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var mu034CheckpointSink []byte
var mu034CheckpointJSONSink []byte

func BenchmarkMU034HistoricalSubscriptionCheckpoint(b *testing.B) {
	resolver := &mu034HistoricalResolver{history: map[uint64][]hatSql.Row{
		1: {{"id": int64(1), "name": "Ada"}},
	}}
	definition := hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
		AsOf:         1,
	}
	registry := hatSql.NewQuerySubscriptions(1)
	historical, err := registry.SubscribeHistorical(context.Background(), definition, resolver, hatSql.QueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer historical.Close()
	ordinary, err := registry.Subscribe(context.Background(), definition, resolver, hatSql.QueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer ordinary.Close()
	checkpoint := historical.Checkpoint()

	b.Run("ordinary_snapshot_control", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, ok := ordinary.Snapshot(); !ok {
				b.Fatal("ordinary snapshot unavailable")
			}
		}
	})
	b.Run("historical_checkpoint_control", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if got := historical.Checkpoint(); got.ID == 0 {
				b.Fatal("historical checkpoint unavailable")
			}
		}
	})
	b.Run("hqs1_binary", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			encoded, err := checkpoint.MarshalBinary()
			if err != nil {
				b.Fatal(err)
			}
			mu034CheckpointSink = encoded
		}
		b.ReportMetric(float64(len(mu034CheckpointSink)), "payload-B/op")
	})
	b.Run("json_compatibility", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			encoded, err := json.Marshal(checkpoint)
			if err != nil {
				b.Fatal(err)
			}
			mu034CheckpointJSONSink = encoded
		}
		b.ReportMetric(float64(len(mu034CheckpointJSONSink)), "payload-B/op")
	})
}
