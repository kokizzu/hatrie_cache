package main

import (
	"bytes"
	"testing"
	"time"

	"hatrie_cache/hat/hatCache"
)

func TestT018ParseConfigJournalSyncPolicy(t *testing.T) {
	defaultConfig, err := parseConfig(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if defaultConfig.journalSyncMode != string(hatCache.DefaultCommandJournalSyncMode) || defaultConfig.journalSyncInterval != 0 {
		t.Fatalf("default sync config = %q/%s, want durable/0", defaultConfig.journalSyncMode, defaultConfig.journalSyncInterval)
	}

	periodicConfig, err := parseConfig([]string{"-journal-sync-mode", "periodic"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if periodicConfig.journalSyncMode != string(hatCache.CommandJournalSyncModePeriodic) || periodicConfig.journalSyncInterval != hatCache.DefaultCommandJournalSyncInterval {
		t.Fatalf("periodic sync config = %q/%s, want periodic/%s", periodicConfig.journalSyncMode, periodicConfig.journalSyncInterval, hatCache.DefaultCommandJournalSyncInterval)
	}
	options := journalOptions(periodicConfig)
	if options.SyncMode != hatCache.CommandJournalSyncModePeriodic || options.SyncInterval != hatCache.DefaultCommandJournalSyncInterval {
		t.Fatalf("journal options = %q/%s, want periodic/%s", options.SyncMode, options.SyncInterval, hatCache.DefaultCommandJournalSyncInterval)
	}

	configured, err := parseConfig([]string{"-journal-sync-mode", "periodic", "-journal-sync-interval", "250ms"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if configured.journalSyncInterval != 250*time.Millisecond {
		t.Fatalf("configured sync interval = %s, want 250ms", configured.journalSyncInterval)
	}

	none, err := parseConfig([]string{"-journal-sync-mode", "none", "-journal-sync-interval", "1s"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if none.journalSyncInterval != 0 {
		t.Fatalf("none sync interval = %s, want 0", none.journalSyncInterval)
	}
	if _, err := parseConfig([]string{"-journal-sync-mode", "invalid"}, &bytes.Buffer{}); err == nil {
		t.Fatal("invalid sync mode was accepted")
	}
	if _, err := parseConfig([]string{"-journal-sync-mode", "periodic", "-journal-sync-interval", "500ns"}, &bytes.Buffer{}); err == nil {
		t.Fatal("too-small sync interval was accepted")
	}
}
