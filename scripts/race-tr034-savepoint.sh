#!/bin/sh
set -eu

go test -race ./hat/hatCache -run 'Test(CompileSQLAtomicProgramSupportsSavepoints|SQLTransactionSavepointsRollbackOnlyLaterWrites)$' -count=1
