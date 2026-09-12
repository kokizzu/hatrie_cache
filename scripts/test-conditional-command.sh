#!/usr/bin/env sh
set -eu

printf '%s\n' 'running conditional command tests'
go test -count=1 -v -run TestExecuteCommandCASString ./hat/hatCache
go test -count=1 -v -run TestCompareAndSwapStringIsAtomic ./hat/hatCache
go test -count=1 -v -run TestCompareAndSwapStringPreservesExpiration ./hat/hatCache
go test -count=1 -v -run TestCompareAndSwapStringDoesNotReplaceOtherTypes ./hat/hatCache
go test -count=1 -v -run TestMonitoringCASJSONCommand ./hat/hatCache
go test -count=1 -v -run TestExecuteCommandCASParticipatesInAtomicBatchRollback ./hat/hatCache
go test -count=1 -v -run TestDataStructureDocumentationCoversValueFamiliesAndCommands ./hat/hatCache
go test -count=1 -v -run TestDataStructureDocumentationHasStateTransitionForEveryCommand ./hat/hatCache
go test -count=1 -v -run TestDataStructureDocumentationListsEveryAcceptedCommandName ./hat/hatCache
go test -count=1 -v -run TestBenchmarkMarkdownTracksExecuteCommand ./hat/hatCache
go test -count=1 -v ./hat/hatCommand -run TestCASExpectedValueRoundTripsThroughProtobuf
