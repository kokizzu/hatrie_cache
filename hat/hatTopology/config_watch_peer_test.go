package hatTopology_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatTopology"
)

func TestConfigWatchPeerReadWaitAndGap(t *testing.T) {
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		HistoryLimit: 4,
		Authorizer: func(_ context.Context, authorization hatTopology.ConfigWatchAuthorization) error {
			if authorization.Principal != "ops" {
				t.Fatalf("authorization principal = %q, want ops", authorization.Principal)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	var lastPayload []byte
	call := func(ctx context.Context, command, payload []byte) ([]byte, error) {
		if string(command) != hatTopology.ConfigWatchPeerCommand {
			t.Fatalf("command = %q, want %q", command, hatTopology.ConfigWatchPeerCommand)
		}
		lastPayload = append(lastPayload[:0], payload...)
		return log.HandleConfigWatchPeerRequest(ctx, payload)
	}
	client, err := hatTopology.NewConfigWatchPeerClient(hatTopology.ConfigWatchPeerClientOptions{
		Call:      call,
		Principal: "ops",
		Prefix:    "feature/",
	})
	if err != nil {
		t.Fatalf("NewConfigWatchPeerClient() error = %v", err)
	}
	if err := log.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{
		Version: 7,
		Source:  "node-a",
		Key:     "feature/cache",
		Value:   []byte("on"),
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	events, cursor, err := client.Read(context.Background(), 0, 4)
	if err != nil || len(events) != 1 || events[0].Version != 7 || string(events[0].Value) != "on" || cursor != 7 {
		t.Fatalf("Read() = events=%#v cursor=%d err=%v, want version 7", events, cursor, err)
	}
	if _, err := log.HandleConfigWatchPeerRequestForPrincipal(context.Background(), "other", lastPayload); !errors.Is(err, hatTopology.ErrConfigWatchPeerPrincipalMismatch) {
		t.Fatalf("principal mismatch error = %v, want ErrConfigWatchPeerPrincipalMismatch", err)
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
		got, next, waitErr := client.Wait(waitContext, 7, 4)
		result <- waitResult{events: got, cursor: next, err: waitErr}
	}()
	select {
	case got := <-result:
		t.Fatalf("Wait() returned before publish: %+v", got)
	case <-time.After(10 * time.Millisecond):
	}
	if err := log.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{
		Version: 8,
		Source:  "node-b",
		Key:     "other/cache",
		Value:   []byte("ignored"),
	}); err != nil {
		t.Fatalf("unrelated Publish() error = %v", err)
	}
	select {
	case got := <-result:
		t.Fatalf("Wait() returned for unrelated key: %+v", got)
	case <-time.After(10 * time.Millisecond):
	}
	if err := log.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{
		Version: 9,
		Source:  "node-b",
		Key:     "feature/cache",
		Value:   []byte("off"),
	}); err != nil {
		t.Fatalf("second Publish() error = %v", err)
	}
	select {
	case got := <-result:
		if got.err != nil || len(got.events) != 1 || got.events[0].Version != 9 || got.cursor != 9 {
			t.Fatalf("Wait() = %+v, want version 9", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait() did not return after publish")
	}

	gapLog, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		HistoryLimit: 1,
		Authorizer: func(context.Context, hatTopology.ConfigWatchAuthorization) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewConfigWatchLog(gap) error = %v", err)
	}
	gapCall := func(ctx context.Context, command, payload []byte) ([]byte, error) {
		if string(command) != hatTopology.ConfigWatchPeerCommand {
			t.Fatalf("gap command = %q, want %q", command, hatTopology.ConfigWatchPeerCommand)
		}
		return gapLog.HandleConfigWatchPeerRequest(ctx, payload)
	}
	gapClient, err := hatTopology.NewConfigWatchPeerClient(hatTopology.ConfigWatchPeerClientOptions{
		Call:      gapCall,
		Principal: "ops",
		Prefix:    "feature/",
	})
	if err != nil {
		t.Fatalf("NewConfigWatchPeerClient(gap) error = %v", err)
	}
	for version := uint64(1); version <= 2; version++ {
		if err := gapLog.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{
			Version: version,
			Source:  "node-a",
			Key:     "feature/gap",
		}); err != nil {
			t.Fatalf("gap Publish(%d) error = %v", version, err)
		}
	}
	_, _, err = gapClient.Read(context.Background(), 0, 1)
	if !errors.Is(err, hatTopology.ErrConfigWatchHistoryGap) {
		t.Fatalf("gap Read() error = %v, want ErrConfigWatchHistoryGap", err)
	}
	var gapErr *hatTopology.ConfigWatchGapError
	if !errors.As(err, &gapErr) || gapErr.EarliestVersion != 2 || gapErr.CurrentVersion != 2 {
		t.Fatalf("gap Read() error = %#v, want earliest/current 2", err)
	}
}

func TestConfigWatchPeerRejectsMalformedFramesAndResponses(t *testing.T) {
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		Authorizer: func(context.Context, hatTopology.ConfigWatchAuthorization) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	invalidPayloads := [][]byte{
		nil,
		{hatTopology.ConfigWatchPeerProtocolVersion},
		{hatTopology.ConfigWatchPeerProtocolVersion, 99},
		{hatTopology.ConfigWatchPeerProtocolVersion, 1, 3, 'o', 'p', 's', 0, 0, 0x80},
	}
	for _, payload := range invalidPayloads {
		if _, err := log.HandleConfigWatchPeerRequest(context.Background(), payload); !errors.Is(err, hatTopology.ErrConfigWatchPeerRequestInvalid) {
			t.Fatalf("HandleConfigWatchPeerRequest(%v) error = %v, want request invalid", payload, err)
		}
	}
	client, err := hatTopology.NewConfigWatchPeerClient(hatTopology.ConfigWatchPeerClientOptions{
		Principal: "ops",
		Call: func(context.Context, []byte, []byte) ([]byte, error) {
			return []byte{hatTopology.ConfigWatchPeerProtocolVersion, 0, 0, 1}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConfigWatchPeerClient() error = %v", err)
	}
	if _, _, err := client.Read(context.Background(), 0, 1); !errors.Is(err, hatTopology.ErrConfigWatchPeerResponseInvalid) {
		t.Fatalf("malformed response error = %v, want response invalid", err)
	}
	smallClient, err := hatTopology.NewConfigWatchPeerClient(hatTopology.ConfigWatchPeerClientOptions{
		Principal:        "ops",
		MaxResponseBytes: 4,
		Call: func(context.Context, []byte, []byte) ([]byte, error) {
			return []byte{1, 0, 0, 0, 0}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConfigWatchPeerClient(small response) error = %v", err)
	}
	if _, _, err := smallClient.Read(context.Background(), 0, 1); !errors.Is(err, hatTopology.ErrConfigWatchPeerResponseTooLarge) {
		t.Fatalf("oversized response error = %v, want response too large", err)
	}
}
