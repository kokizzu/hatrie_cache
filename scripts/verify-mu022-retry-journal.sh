#!/usr/bin/env bash
set -euo pipefail

test -s MU022_CONNECTOR_TRANSACTION_RETRY_JOURNAL.md
test -s hat/hatSql/mu022_connector_transaction_retry_journal.go
test -s hat/hatSql/mu022_retry_journal_test.go
rg -n 'SQLConnectorTransactionRetryJournal|mu-022-connector-transaction-retry-journal' README.md MU022_CONNECTOR_TRANSACTION_RETRY_JOURNAL.md BENCHMARK.md PRODUCT_IDEA_GAPS.md
rg -n 'M-U22.*Adopted' PRODUCT_IDEA_GAPS.md
