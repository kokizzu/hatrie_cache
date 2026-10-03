package hatTopology_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func TestConfigWatchApplyReplicatedIsIdempotentAndOrdered(t *testing.T) {
	var actions []hatTopology.ConfigWatchAction
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		Authorizer: func(_ context.Context, authorization hatTopology.ConfigWatchAuthorization) error {
			actions = append(actions, authorization.Action)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	event := hatTopology.ConfigWatchEvent{
		Version: 100,
		Source:  "node-a",
		Key:     "feature/cache",
		Value:   []byte("on"),
	}
	applied, err := log.ApplyReplicated(context.Background(), "replicator", event)
	if err != nil || !applied {
		t.Fatalf("first ApplyReplicated() = applied=%t err=%v, want true", applied, err)
	}

	duplicate := event
	duplicate.Value = append([]byte(nil), event.Value...)
	applied, err = log.ApplyReplicated(context.Background(), "replicator", duplicate)
	if err != nil || applied {
		t.Fatalf("duplicate ApplyReplicated() = applied=%t err=%v, want false/nil", applied, err)
	}

	conflict := event
	conflict.Value = []byte("off")
	if applied, err := log.ApplyReplicated(context.Background(), "replicator", conflict); !errors.Is(err, hatTopology.ErrConfigWatchReplicationConflict) || applied {
		t.Fatalf("conflicting ApplyReplicated() = applied=%t err=%v, want conflict", applied, err)
	}

	gap := event
	gap.Version++
	gap.Version++
	if applied, err := log.ApplyReplicated(context.Background(), "replicator", gap); !errors.Is(err, hatTopology.ErrConfigWatchVersionGap) || applied {
		t.Fatalf("gapped ApplyReplicated() = applied=%t err=%v, want gap", applied, err)
	}

	next := event
	next.Version++
	next.Value = []byte("off")
	applied, err = log.ApplyReplicated(context.Background(), "replicator", next)
	if err != nil || !applied {
		t.Fatalf("next ApplyReplicated() = applied=%t err=%v, want true", applied, err)
	}

	events, cursor, err := log.Read(context.Background(), hatTopology.ConfigWatchRequest{
		Principal:    "reader",
		AfterVersion: 99,
		Limit:        4,
	})
	if err != nil || cursor != 101 || len(events) != 2 || !bytes.Equal(events[0].Value, []byte("on")) || !bytes.Equal(events[1].Value, []byte("off")) {
		t.Fatalf("replicated Read() = events=%#v cursor=%d err=%v, want versions 100/101", events, cursor, err)
	}
	if len(actions) != 6 || actions[0] != hatTopology.ConfigWatchReplicate || actions[1] != hatTopology.ConfigWatchReplicate || actions[2] != hatTopology.ConfigWatchReplicate || actions[3] != hatTopology.ConfigWatchReplicate || actions[4] != hatTopology.ConfigWatchReplicate || actions[5] != hatTopology.ConfigWatchRead {
		t.Fatalf("authorization actions = %#v, want five replicate/read actions", actions)
	}
}

func TestConfigWatchApplyReplicatedRequiresVersion(t *testing.T) {
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{Authorizer: allowConfigWatch})
	if err != nil {
		t.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	if applied, err := log.ApplyReplicated(context.Background(), "replicator", hatTopology.ConfigWatchEvent{Source: "node-a", Key: "feature/cache"}); !errors.Is(err, hatTopology.ErrConfigWatchReplicationVersionRequired) || applied {
		t.Fatalf("zero-version ApplyReplicated() = applied=%t err=%v, want version error", applied, err)
	}
}

func BenchmarkConfigWatchApplyReplicated(b *testing.B) {
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		HistoryLimit: 256,
		Authorizer:   allowConfigWatch,
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := log.ApplyReplicated(ctx, "bench", hatTopology.ConfigWatchEvent{
			Version: uint64(index + 1),
			Source:  "node-a",
			Key:     "feature/cache",
			Value:   []byte("on"),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConfigWatchApplyReplicatedDuplicate(b *testing.B) {
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		HistoryLimit: 256,
		Authorizer:   allowConfigWatch,
	})
	if err != nil {
		b.Fatal(err)
	}
	event := hatTopology.ConfigWatchEvent{Version: 1, Source: "node-a", Key: "feature/cache", Value: []byte("on")}
	if _, err := log.ApplyReplicated(context.Background(), "bench", event); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if applied, err := log.ApplyReplicated(ctx, "bench", event); err != nil || applied {
			b.Fatalf("duplicate ApplyReplicated() = applied=%t err=%v", applied, err)
		}
	}
}
