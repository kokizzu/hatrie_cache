#!/usr/bin/env bash
set -euo pipefail

rg -n -C 4 'ttlNow|setTypedTableTTLDeadlineLocked|expiry' \
	hat/hatSql/typed_table.go hat/hatSql/typed_table_ttl.go hat/hatSql/typed_table_patch_parts.go
