package hatReplication

import (
	"context"
	"testing"
)

var (
	m229SchemaPrevious = benchmarkChangefeedSchema("orders-v1", 256)
	m229SchemaNext     = benchmarkChangefeedSchema("orders-v2", 264)
	m229SchemaSink     ChangefeedSchemaCompatibility
)

func init() {
	for index := 256; index < len(m229SchemaNext.Fields); index++ {
		m229SchemaNext.Fields[index].Name = "added_" + m229SchemaNext.Fields[index].Name
		m229SchemaNext.Fields[index].Nullable = true
	}
}

func BenchmarkChangefeedSchemaEvolutionCheck(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		change, err := CheckChangefeedSchemaEvolution(m229SchemaPrevious, m229SchemaNext)
		if err != nil {
			b.Fatal(err)
		}
		m229SchemaSink = change
	}
}

func BenchmarkChangefeedSchemaCompatibilityCheck(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if err := CheckChangefeedSchemaCompatibility(m229SchemaNext, m229SchemaPrevious); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSpaceChangefeedPublishLegacySchema(b *testing.B) {
	feed, subscription := benchmarkSpaceChangefeed(b, false)
	benchmarkSpaceChangefeedPublish(b, feed, subscription)
}

func BenchmarkSpaceChangefeedPublishTypedSchema(b *testing.B) {
	feed, subscription := benchmarkSpaceChangefeed(b, true)
	benchmarkSpaceChangefeedPublish(b, feed, subscription)
}

func benchmarkSpaceChangefeedPublish(b *testing.B, feed *SpaceChangefeed, subscription *SpaceChangefeedSubscription) {
	event := SpaceChangefeedEvent{Operation: SpaceChangefeedUpsert, Key: []byte("order-1")}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := feed.Publish(event); err != nil {
			b.Fatal(err)
		}
		if _, ok := <-subscription.Events(); !ok {
			b.Fatal("subscription closed during benchmark")
		}
	}
}

func benchmarkSpaceChangefeed(b *testing.B, typed bool) (*SpaceChangefeed, *SpaceChangefeedSubscription) {
	b.Helper()
	options := SpaceChangefeedOptions{Space: "orders", SchemaVersion: "orders-v1", MaxEvents: 1, MaxBytes: 1 << 20}
	subscribe := SpaceChangefeedSubscribeOptions{ExpectedSchemaVersion: "orders-v1", Buffer: 1}
	if typed {
		schema := m229SchemaPrevious
		options.Schema = &schema
		subscribe.ExpectedSchemaVersion = ""
		subscribe.ExpectedSchema = &schema
	}
	feed, err := NewSpaceChangefeed(options)
	if err != nil {
		b.Fatal(err)
	}
	subscription, err := feed.Subscribe(context.Background(), subscribe)
	if err != nil {
		b.Fatal(err)
	}
	return feed, subscription
}

func benchmarkChangefeedSchema(version string, fieldCount int) ChangefeedSchema {
	schema := ChangefeedSchema{Version: version, Fields: make([]ChangefeedSchemaField, fieldCount)}
	for index := range schema.Fields {
		schema.Fields[index] = ChangefeedSchemaField{Name: "field_" + benchmarkChangefeedSchemaIndex(index), Type: "string", Nullable: true}
	}
	schema.Fields[0] = ChangefeedSchemaField{Name: "id", Type: "int64"}
	return schema
}

func benchmarkChangefeedSchemaIndex(index int) string {
	const digits = "0123456789"
	var value [20]byte
	position := len(value)
	if index == 0 {
		return "0"
	}
	for index > 0 {
		position--
		value[position] = digits[index%10]
		index /= 10
	}
	return string(value[position:])
}
