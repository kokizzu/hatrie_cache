#!/bin/sh
set -eu

gofmt -w \
	hat/hatJournal/encryption.go \
	hat/hatJournal/journal.go \
	hat/hatJournal/reader.go \
	hat/hatJournal/tr008_encryption_test.go \
	hat/hatJournal/tr008_encryption_benchmark_test.go \
	hat/hatCache/journal.go \
	hat/hatCache/journal_segments.go \
	hat/hatCache/journal_subscription.go \
	hat/hatCache/tr008_journal_encryption_test.go \
	hat/hatCache/tr008_journal_checkpoint_test.go
