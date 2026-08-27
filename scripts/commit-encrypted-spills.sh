#!/bin/sh
set -eu

git add Makefile hat/hatCodec/stream_cipher.go hat/hatCodec/stream_cipher_test.go hat/hatSql/query.go hat/hatSql/spill_encryption_test.go scripts/commit-encrypted-spills.sh scripts/format-sql-spill-encryption.sh scripts/format-stream-cipher.sh scripts/push-encrypted-spills.sh scripts/test-sql-spill-encryption.sh scripts/test-stream-cipher.sh
git commit -m 'feat: encrypt authenticated SQL spill files'
