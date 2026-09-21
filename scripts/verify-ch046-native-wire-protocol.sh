#!/usr/bin/env bash
set -euo pipefail

test -f CH046_NATIVE_WIRE_PROTOCOL.md
test -f hat/hatSql/columnar_block_stream.go
test -f hat/hatSql/ch046_native_wire_protocol_test.go
test -f hat/hatCache/sql_columnar_block_http.go
test -f hat/hatCache/sql_columnar_block_http_test.go
test -f hat/hatCache/ch046_native_wire_protocol_benchmark_test.go
bash -n scripts/ch046-native-wire-protocol.sh scripts/verify-ch046-native-wire-protocol.sh scripts/stage-ch046-native-wire-protocol.sh scripts/commit-ch046-native-wire-protocol.sh scripts/push-ch046-native-wire-protocol.sh

rg -q 'SQLColumnarBlockStreamContentType' hat/hatSql/columnar_block_stream.go
rg -q 'NextBlockFields' hat/hatSql/columnar_block_stream.go
rg -q 'monitoringSQLRequestAcceptsColumnar' hat/hatCache/sql_columnar_block_http.go
rg -q 'CH-046.*Partially adopted' ENGINE_IDEAS.md
rg -q 'CH046_NATIVE_WIRE_PROTOCOL.md' README.md
rg -q 'make benchmark-ch046-native-wire-protocol' CH046_NATIVE_WIRE_PROTOCOL.md

test ! -e scripts/inspect-ch046-wire-protocol.sh
test ! -e scripts/list-open-inspiration-ideas.sh
git diff --check
git diff --cached --check
