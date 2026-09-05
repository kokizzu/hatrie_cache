#!/bin/sh
set -eu

go test ./hat/hatSql -run 'Test(SQLColumnarSparsePrimarySegmentRange|TypedTableColumnarSparsePrimaryIndex|SQLColumnarNumericAggregateUsesSegmentedBatchWhenAvailable)' -count=1
