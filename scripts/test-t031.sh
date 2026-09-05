#!/bin/sh
set -eu

go test ./hat/hatCache -run 'Test(CompileSQLAtomicProgramSupportsSavepoints|SQLTransactionSavepointsRollbackOnlyLaterWrites)$' -count=1
