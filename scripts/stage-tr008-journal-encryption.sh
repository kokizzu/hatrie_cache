#!/bin/sh
set -eu

git add \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	README.md \
	TR008_WAL_ENCRYPTION.md \
	hat/hatCache/journal.go \
	hat/hatCache/journal_segments.go \
	hat/hatCache/journal_subscription.go \
	hat/hatCache/tr008_journal_checkpoint_test.go \
	hat/hatCache/tr008_journal_encryption_test.go \
	hat/hatJournal/encryption.go \
	hat/hatJournal/journal.go \
	hat/hatJournal/reader.go \
	hat/hatJournal/tr008_encryption_benchmark_test.go \
	hat/hatJournal/tr008_encryption_test.go \
	scripts/benchmark-tr008-journal-encryption.sh \
	scripts/commit-tr008-journal-encryption.sh \
	scripts/format-tr008-journal-encryption.sh \
	scripts/race-tr008-journal-encryption.sh \
	scripts/review-tr008-journal-encryption.sh \
	scripts/status-tr008-journal-encryption.sh \
	scripts/stage-tr008-journal-encryption.sh \
	scripts/test-tr008-journal-encryption.sh \
	scripts/test-tr008-package.sh \
	scripts/vet-tr008-journal-encryption.sh
