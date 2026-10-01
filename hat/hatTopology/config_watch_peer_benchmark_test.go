package hatTopology

import (
	"context"
	"encoding/json"
	"testing"
)

type configWatchPeerJSONRequest struct {
	Operation    string `json:"operation"`
	Principal    string `json:"principal"`
	Prefix       string `json:"prefix,omitempty"`
	AfterVersion uint64 `json:"after_version"`
	Limit        int    `json:"limit"`
}

type configWatchPeerJSONResponse struct {
	Cursor uint64             `json:"cursor"`
	Events []ConfigWatchEvent `json:"events,omitempty"`
}

func BenchmarkConfigWatchPeerBinaryRoundTrip(b *testing.B) {
	request := configWatchPeerRequest{
		operation:    configWatchPeerOperationRead,
		principal:    "ops",
		prefix:       "feature/",
		afterVersion: 128,
		limit:        4,
	}
	events := configWatchPeerBenchmarkEvents()
	requestPayload, err := encodeConfigWatchPeerRequest(request)
	if err != nil {
		b.Fatal(err)
	}
	responsePayload, err := encodeConfigWatchPeerEvents(events, 132, DefaultConfigWatchPeerMaxResponseBytes)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(requestPayload) + len(responsePayload)))
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(requestPayload)), "request-bytes/op")
	b.ReportMetric(float64(len(responsePayload)), "response-bytes/op")
	for i := 0; i < b.N; i++ {
		if _, err := decodeConfigWatchPeerRequest(requestPayload); err != nil {
			b.Fatal(err)
		}
		if _, _, err := decodeConfigWatchPeerResponse(responsePayload, 128); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConfigWatchPeerJSONRoundTrip(b *testing.B) {
	request := configWatchPeerJSONRequest{
		Operation:    "read",
		Principal:    "ops",
		Prefix:       "feature/",
		AfterVersion: 128,
		Limit:        4,
	}
	response := configWatchPeerJSONResponse{Cursor: 132, Events: configWatchPeerBenchmarkEvents()}
	requestPayload, err := json.Marshal(request)
	if err != nil {
		b.Fatal(err)
	}
	responsePayload, err := json.Marshal(response)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(requestPayload) + len(responsePayload)))
	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(requestPayload)), "request-bytes/op")
	b.ReportMetric(float64(len(responsePayload)), "response-bytes/op")
	for i := 0; i < b.N; i++ {
		var decodedRequest configWatchPeerJSONRequest
		if err := json.Unmarshal(requestPayload, &decodedRequest); err != nil {
			b.Fatal(err)
		}
		var decodedResponse configWatchPeerJSONResponse
		if err := json.Unmarshal(responsePayload, &decodedResponse); err != nil {
			b.Fatal(err)
		}
	}
}

func configWatchPeerBenchmarkEvents() []ConfigWatchEvent {
	return []ConfigWatchEvent{
		{Version: 129, Source: "node-a", Key: "feature/cache", Value: []byte("on")},
		{Version: 130, Source: "node-b", Key: "feature/region", Value: []byte("sg")},
		{Version: 131, Source: "node-a", Key: "feature/queue", Value: []byte("bounded")},
		{Version: 132, Source: "node-c", Key: "feature/retry", Value: []byte("3")},
	}
}

func BenchmarkConfigWatchPeerEndToEndHandler(b *testing.B) {
	log, err := NewConfigWatchLog(ConfigWatchOptions{
		Authorizer: func(context.Context, ConfigWatchAuthorization) error { return nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, event := range configWatchPeerBenchmarkEvents() {
		if err := log.Publish(context.Background(), "bench", event); err != nil {
			b.Fatal(err)
		}
	}
	payload, err := encodeConfigWatchPeerRequest(configWatchPeerRequest{
		operation:    configWatchPeerOperationRead,
		principal:    "bench",
		afterVersion: 128,
		limit:        4,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := log.HandleConfigWatchPeerRequest(context.Background(), payload); err != nil {
			b.Fatal(err)
		}
	}
}
