#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLColumnarScanUses(MixedVectorConjunction|DictionaryNumericConjunction)$'
