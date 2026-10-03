package hatJournal

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

func TestSpaceSyncAppenderAppliesPerSpacePolicies(t *testing.T) {
	registry, err := NewSpaceSyncPolicyRegistry(SpaceSyncPolicyOptions{
		Capacity:      2,
		DefaultPolicy: SpaceSyncPolicyPeriodic,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders", SpaceSyncPolicyImmediate); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("cache", SpaceSyncPolicyDisabled); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	var syncs int
	now := time.Unix(100, 0)
	appender, err := NewSpaceSyncAppender(&output, func() error {
		syncs++
		return nil
	}, SpaceSyncAppenderOptions{
		Registry:         registry,
		PeriodicInterval: time.Hour,
		PeriodicMaxBytes: 4,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := appender.Append("orders", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Policy != SpaceSyncPolicyImmediate || !result.Synced || syncs != 1 {
		t.Fatalf("immediate append result = %#v, syncs = %d", result, syncs)
	}

	result, err = appender.Append("cache", []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Policy != SpaceSyncPolicyDisabled || result.Synced || syncs != 1 {
		t.Fatalf("disabled append result = %#v, syncs = %d", result, syncs)
	}

	result, err = appender.Append("events", []byte("tri"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Policy != SpaceSyncPolicyPeriodic || result.Synced || syncs != 1 {
		t.Fatalf("periodic append result = %#v, syncs = %d", result, syncs)
	}

	flushed, err := appender.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if !flushed || syncs != 2 {
		t.Fatalf("Flush() = %v, syncs = %d, want one pending sync", flushed, syncs)
	}
	if got := output.String(); got != "onetwotri" {
		t.Fatalf("output = %q", got)
	}
}

func TestSpaceSyncAppenderPeriodicDeadlineAndSyncRetry(t *testing.T) {
	var output bytes.Buffer
	now := time.Unix(200, 0)
	var syncs int
	var fail bool
	appender, err := NewSpaceSyncAppender(&output, func() error {
		syncs++
		if fail {
			return errors.New("injected sync failure")
		}
		return nil
	}, SpaceSyncAppenderOptions{
		PeriodicInterval: time.Second,
		Now:              func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := appender.Append("events", []byte("a")); err != nil || result.Synced {
		t.Fatalf("initial append = %#v/%v, want pending periodic write", result, err)
	}
	now = now.Add(2 * time.Second)
	fail = true
	if _, err := appender.Append("events", []byte("b")); err == nil {
		t.Fatal("deadline sync unexpectedly succeeded")
	}
	fail = false
	flushed, err := appender.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if !flushed || syncs != 2 {
		t.Fatalf("retry Flush() = %v, syncs = %d, want failed deadline plus successful retry", flushed, syncs)
	}
}

func TestSpaceSyncAppenderValidatesConfigurationAndShortWrites(t *testing.T) {
	if _, err := NewSpaceSyncAppender(nil, func() error { return nil }, SpaceSyncAppenderOptions{}); !errors.Is(err, ErrSpaceSyncAppenderWriterNil) {
		t.Fatalf("nil writer error = %v", err)
	}
	var output bytes.Buffer
	if _, err := NewSpaceSyncAppender(&output, nil, SpaceSyncAppenderOptions{}); !errors.Is(err, ErrSpaceSyncAppenderSyncNil) {
		t.Fatalf("nil sync error = %v", err)
	}
	for _, options := range []SpaceSyncAppenderOptions{
		{PeriodicInterval: -time.Second},
		{PeriodicMaxBytes: -1},
		{PeriodicMaxBytes: MaxSpaceSyncAppenderPeriodicMaxBytes + 1},
	} {
		if _, err := NewSpaceSyncAppender(&output, func() error { return nil }, options); !errors.Is(err, ErrSpaceSyncAppenderConfiguration) {
			t.Fatalf("options %#v error = %v", options, err)
		}
	}

	appender, err := NewSpaceSyncAppender(shortWriter{}, func() error {
		t.Fatal("short write must not sync")
		return nil
	}, SpaceSyncAppenderOptions{Registry: mustSpaceSyncPolicyRegistry(t, SpaceSyncPolicyImmediate)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := appender.Append("orders", []byte("payload"))
	if !errors.Is(err, io.ErrShortWrite) || result.BytesWritten != 1 || result.Synced {
		t.Fatalf("short append = %#v/%v", result, err)
	}
}

type shortWriter struct{}

func (shortWriter) Write(payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, nil
	}
	return 1, nil
}

func mustSpaceSyncPolicyRegistry(t *testing.T, defaultPolicy SpaceSyncPolicy) *SpaceSyncPolicyRegistry {
	t.Helper()
	registry, err := NewSpaceSyncPolicyRegistry(SpaceSyncPolicyOptions{DefaultPolicy: defaultPolicy})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
