#!/usr/bin/env sh
set -eu

go test ./hat/hatSql ./hat/hatCache -run '^(TestParserPlannerMutationCorpusRejectsInvalidVariants|TestSeededSQLCancellationWorkload|TestSeededReadWriteRecoveryWorkload)$'
