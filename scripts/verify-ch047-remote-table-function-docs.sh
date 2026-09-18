#!/usr/bin/env bash
set -eu

test -s CH047_REMOTE_TABLE_FUNCTIONS.md
grep -q 'CH-47' CH047_REMOTE_TABLE_FUNCTIONS.md
grep -q 'ch-47-s3-and-url-table-functions' BENCHMARK.md
grep -q 'CH047_REMOTE_TABLE_FUNCTIONS.md' README.md
grep -q '| CH-47 | S3 and URL table functions' INSPIRATION_BACKLOG.md
