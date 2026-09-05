#!/bin/sh
set -eu

go test ./hat/hatCache -run 'TestExecute(CommandAtomicScalarBatchRollsBackAfterRuntimeFailure|PublicAtomicCommandBatchRollsBackMemoryAndJournal)$' -count=1
