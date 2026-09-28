package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

var m206BenchmarkSink interface{}

func BenchmarkM206ExistingCDCNormalize(b *testing.B) {
	envelope := hatSql.CDCEnvelope{
		Sequence:  42,
		Operation: "UPDATE",
		Key:       "customer-42",
		Before:    hatSql.Row{"name": "Ada", "score": int64(7)},
		After:     hatSql.Row{"name": "Grace", "score": int64(8)},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		change, err := hatSql.NormalizeCDCEnvelope(envelope)
		if err != nil {
			b.Fatal(err)
		}
		m206BenchmarkSink = change
	}
}

func BenchmarkM206UpsertNormalize(b *testing.B) {
	envelope := hatSql.UpsertEnvelope{
		Sequence: 42,
		Key:      "customer-42",
		Row:      hatSql.Row{"name": "Grace", "score": int64(8)},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		change, err := hatSql.NormalizeUpsertEnvelope(envelope)
		if err != nil {
			b.Fatal(err)
		}
		m206BenchmarkSink = change
	}
}

func BenchmarkM206ExistingCDCJSONDecode(b *testing.B) {
	payload := []byte(`{"sequence":42,"op":"UPDATE","key":"customer-42","before":{"name":"Ada","score":7},"after":{"name":"Grace","score":8}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		change, err := hatSql.DecodeCDCEnvelopeJSON(payload)
		if err != nil {
			b.Fatal(err)
		}
		m206BenchmarkSink = change
	}
}

func BenchmarkM206UpsertJSONDecode(b *testing.B) {
	payload := []byte(`{"sequence":42,"key":"customer-42","row":{"name":"Grace","score":8}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		change, err := hatSql.DecodeUpsertEnvelopeJSON(payload)
		if err != nil {
			b.Fatal(err)
		}
		m206BenchmarkSink = change
	}
}
