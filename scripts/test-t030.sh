#!/bin/sh
set -eu

go test ./hat/hatCache -run 'Test(BeginSQLTransactionWithOptionsSupportsSerializableIsolation|SerializableTransactionRejectsConcurrentTypedMutation|BeginSQLTransactionDefaultRemainsOptimisticSnapshot|ParseSQLTransactionIsolation)$' -count=1
