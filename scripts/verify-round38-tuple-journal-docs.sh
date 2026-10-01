#!/usr/bin/env bash
set -euo pipefail

grep -Fq 'OpenTupleFieldOperationJournal' TU19_TUPLE_OPERATION_JOURNAL.md
grep -Fq 'UnsafeNoSync' TU19_TUPLE_OPERATION_JOURNAL.md
grep -Fq '1137479' TU19_TUPLE_OPERATION_JOURNAL.md
grep -Fq 'TU19_TUPLE_OPERATION_JOURNAL.md' README.md
grep -Fq 'T-U19' PRODUCT_IDEA_GAPS.md
grep -Fq 'Journal, default fsync' BENCHMARK.md
