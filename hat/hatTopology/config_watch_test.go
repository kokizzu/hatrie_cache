package hatTopology_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatTopology"
)

var configWatchDenied = errors.New("config watch access denied")

func allowConfigWatch(context.Context, hatTopology.ConfigWatchAuthorization) error {
	return nil
}

func TestConfigWatchPublishesReplaysAndWaitsForResume(t *testing.T) {
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		HistoryLimit: 4,
		Authorizer:   allowConfigWatch,
	})
	if err != nil {
		t.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	value := []byte("on")
	if err := log.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{
		Version: 10,
		Source:  "node-a",
		Key:     "feature/cache",
		Value:   value,
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	value[0] = 'x'
	events, cursor, err := log.Read(context.Background(), hatTopology.ConfigWatchRequest{
		Principal:    "ops",
		AfterVersion: 0,
		Limit:        4,
	})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(events) != 1 || events[0].Version != 10 || string(events[0].Value) != "on" || cursor != 10 {
		t.Fatalf("replay = %#v cursor=%d, want version 10/value on", events, cursor)
	}

	waitContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	type waitResult struct {
		events []hatTopology.ConfigWatchEvent
		cursor uint64
		err    error
	}
	result := make(chan waitResult, 1)
	go func() {
		got, next, waitErr := log.Wait(waitContext, hatTopology.ConfigWatchRequest{
			Principal:    "ops",
			AfterVersion: 10,
			Limit:        4,
		})
		result <- waitResult{events: got, cursor: next, err: waitErr}
	}()
	select {
	case got := <-result:
		t.Fatalf("Wait() returned before publish: %+v", got)
	case <-time.After(10 * time.Millisecond):
	}
	if err := log.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{
		Version: 11,
		Source:  "node-b",
		Key:     "feature/cache",
		Value:   []byte("off"),
	}); err != nil {
		t.Fatalf("second Publish() error = %v", err)
	}
	select {
	case got := <-result:
		if got.err != nil || len(got.events) != 1 || got.events[0].Version != 11 || got.cursor != 11 {
			t.Fatalf("Wait() result = %+v, want version 11", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait() did not wake after publish")
	}
}

func TestConfigWatchBoundsHistoryAndReportsGap(t *testing.T) {
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		HistoryLimit: 2,
		Authorizer:   allowConfigWatch,
	})
	if err != nil {
		t.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	for version := uint64(1); version <= 3; version++ {
		if err := log.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{
			Version: version,
			Source:  "node-a",
			Key:     "feature/" + string(rune('a'+version)),
		}); err != nil {
			t.Fatalf("Publish(%d) error = %v", version, err)
		}
	}
	stats := log.Stats()
	if stats.CurrentVersion != 3 || stats.EarliestVersion != 2 || stats.Retained != 2 {
		t.Fatalf("Stats() = %+v, want current=3 earliest=2 retained=2", stats)
	}
	_, _, err = log.Read(context.Background(), hatTopology.ConfigWatchRequest{Principal: "ops", AfterVersion: 0, Limit: 2})
	if !errors.Is(err, hatTopology.ErrConfigWatchHistoryGap) {
		t.Fatalf("gap error = %v, want ErrConfigWatchHistoryGap", err)
	}
	var gapErr *hatTopology.ConfigWatchGapError
	if !errors.As(err, &gapErr) || gapErr.EarliestVersion != 2 {
		t.Fatalf("gap error = %#v, want earliest version 2", err)
	}
	events, cursor, err := log.Read(context.Background(), hatTopology.ConfigWatchRequest{Principal: "ops", AfterVersion: 1, Limit: 1})
	if err != nil || len(events) != 1 || events[0].Version != 2 || cursor != 2 {
		t.Fatalf("bounded Read() = events=%#v cursor=%d err=%v, want version 2", events, cursor, err)
	}
}

func TestConfigWatchAuthorizationVersionAndCancellation(t *testing.T) {
	var mu sync.Mutex
	var authorizations []hatTopology.ConfigWatchAuthorization
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		Authorizer: func(_ context.Context, authorization hatTopology.ConfigWatchAuthorization) error {
			mu.Lock()
			authorizations = append(authorizations, authorization)
			mu.Unlock()
			if authorization.Principal == "denied" {
				return configWatchDenied
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	deniedEvent := hatTopology.ConfigWatchEvent{Version: 1, Source: "node-a", Key: "feature/a"}
	if err := log.Publish(context.Background(), "denied", deniedEvent); !errors.Is(err, configWatchDenied) {
		t.Fatalf("denied Publish() error = %v, want denial", err)
	}
	if _, _, err := log.Read(context.Background(), hatTopology.ConfigWatchRequest{Principal: "denied", Limit: 1}); !errors.Is(err, configWatchDenied) {
		t.Fatalf("denied Read() error = %v, want denial", err)
	}
	if err := log.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{Version: 5, Source: "node-a", Key: "feature/a"}); err != nil {
		t.Fatalf("version 5 Publish() error = %v", err)
	}
	if err := log.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{Version: 5, Source: "node-a", Key: "feature/b"}); !errors.Is(err, hatTopology.ErrConfigWatchVersionStale) {
		t.Fatalf("stale Publish() error = %v, want ErrConfigWatchVersionStale", err)
	}
	if err := log.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{Source: "node-a", Key: "feature/b"}); err != nil {
		t.Fatalf("auto-version Publish() error = %v", err)
	}
	if got := log.Stats().CurrentVersion; got != 6 {
		t.Fatalf("CurrentVersion = %d, want 6", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := log.Wait(ctx, hatTopology.ConfigWatchRequest{Principal: "ops", AfterVersion: 6, Limit: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Wait() error = %v, want context.Canceled", err)
	}
	mu.Lock()
	if len(authorizations) < 4 {
		t.Fatalf("authorization calls = %d, want publish/read coverage", len(authorizations))
	}
	mu.Unlock()
}

func TestConfigWatchValidatesOptionsEventsAndCopiesReads(t *testing.T) {
	invalidOptions := []hatTopology.ConfigWatchOptions{
		{HistoryLimit: -1, Authorizer: allowConfigWatch},
		{HistoryLimit: hatTopology.MaxConfigWatchHistoryLimit + 1, Authorizer: allowConfigWatch},
		{MaxKeyBytes: -1, Authorizer: allowConfigWatch},
		{MaxValueBytes: -1, Authorizer: allowConfigWatch},
		{Authorizer: nil},
	}
	for _, options := range invalidOptions {
		if _, err := hatTopology.NewConfigWatchLog(options); err == nil {
			t.Fatalf("NewConfigWatchLog(%+v) succeeded, want validation error", options)
		}
	}
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		MaxKeyBytes:   8,
		MaxValueBytes: 4,
		Authorizer:    allowConfigWatch,
	})
	if err != nil {
		t.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	invalidEvents := []hatTopology.ConfigWatchEvent{
		{Version: 1, Source: "node-a", Key: ""},
		{Version: 1, Source: "", Key: "a"},
		{Version: 1, Source: "node-a", Key: "123456789"},
		{Version: 1, Source: "node-a", Key: "a", Value: []byte("12345")},
		{Version: 1, Source: "node-a", Key: "a", Deleted: true, Value: []byte("x")},
	}
	for _, event := range invalidEvents {
		if err := log.Publish(context.Background(), "ops", event); err == nil {
			t.Fatalf("Publish(%+v) succeeded, want validation error", event)
		}
	}
	input := []byte("abc")
	if err := log.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{Version: 1, Source: "node-a", Key: "a", Value: input}); err != nil {
		t.Fatalf("valid Publish() error = %v", err)
	}
	input[0] = 'z'
	events, _, err := log.Read(context.Background(), hatTopology.ConfigWatchRequest{Principal: "ops", Limit: 1})
	if err != nil || len(events) != 1 || string(events[0].Value) != "abc" {
		t.Fatalf("copied Read() = %#v err=%v, want abc", events, err)
	}
	events[0].Value[0] = 'x'
	events, _, err = log.Read(context.Background(), hatTopology.ConfigWatchRequest{Principal: "ops", Limit: 1})
	if err != nil || len(events) != 1 || string(events[0].Value) != "abc" {
		t.Fatalf("second copied Read() = %#v err=%v, want abc", events, err)
	}
	if _, _, err := log.Read(context.Background(), hatTopology.ConfigWatchRequest{Principal: "ops", Limit: 0}); err != nil {
		t.Fatalf("zero read limit error = %v, want default", err)
	}
	if strings.TrimSpace("ops") != "ops" {
		t.Fatal("test principal normalization changed unexpectedly")
	}
}

func BenchmarkConfigWatchPublishRead(b *testing.B) {
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		HistoryLimit: 64,
		Authorizer:   allowConfigWatch,
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		version := uint64(i + 1)
		if err := log.Publish(ctx, "bench", hatTopology.ConfigWatchEvent{Version: version, Source: "node-a", Key: "feature/a", Value: []byte("on")}); err != nil {
			b.Fatal(err)
		}
		if _, _, err := log.Read(ctx, hatTopology.ConfigWatchRequest{Principal: "bench", AfterVersion: version - 1, Limit: 1}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConfigWatchWait(b *testing.B) {
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		HistoryLimit: 64,
		Authorizer:   allowConfigWatch,
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		version := uint64(i + 1)
		if err := log.Publish(ctx, "bench", hatTopology.ConfigWatchEvent{Version: version, Source: "node-a", Key: "feature/a"}); err != nil {
			b.Fatal(err)
		}
		if _, _, err := log.Wait(ctx, hatTopology.ConfigWatchRequest{Principal: "bench", AfterVersion: version - 1, Limit: 1}); err != nil {
			b.Fatal(err)
		}
	}
}
