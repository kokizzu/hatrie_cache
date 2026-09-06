#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestColumnarBatch(SelectsDictionaryForWideRepeatedValues|KeepsNarrowUniqueValuesPlain)$'
