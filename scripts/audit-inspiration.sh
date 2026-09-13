#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '--- open inspiration candidates ---'
rg -n '\| (CH|MZ|TR)-[0-9]+ .*\| \[ \] \|' INSPIRATION_BACKLOG.md
printf '%s\n' '--- mutation response and returning APIs ---'
rg -n 'OldValue|NewValue|Old.*Tuple|New.*Tuple|RETURNING|returning|Mutation|Mutate' hat/hatCache hat/hatSql hat/hatDataStructure --glob '*.go'
printf '%s\n' '--- asynchronous insert and bounded ingestion APIs ---'
rg -n 'AsyncInsert|InsertBuffer|Insert.*Batch|Batch.*Insert|Flush.*Batch|SubmitAsync|GroupCommit|UpsertBatch' hat/hatCache hat/hatSql hat/hatDataStructure --glob '*.go'
printf '%s\n' '--- reverse scan and incremental top-k APIs ---'
rg -n 'Reverse|reverse|TopN|Top-K|RankWindow|rank window|Order.*Desc|SeekAfter|SnapshotCursor' hat/hatCache hat/hatSql hat/hatDataStructure --glob '*.go'
printf '%s\n' '--- tuple layout and field access implementation ---'
rg --files hat/hatDataStructure | rg 'tuple|Tuple'
rg -n 'Tuple|FieldOffset|fieldOffset|Field.*Offset|Parse.*Tuple|Encode.*Tuple|Decode.*Tuple' hat/hatDataStructure --glob '*.go' --glob '!*_test.go'
printf '%s\n' '--- direct command response mutation fields ---'
rg -n 'Old|New|Previous|Before|After|Returning|RETURNING' hat/hatCache/command*.go hat/hatCache/journal*.go hat/hatCommand --glob '*.go' --glob '!*_test.go'
