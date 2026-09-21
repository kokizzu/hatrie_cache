#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md CH046_NATIVE_WIRE_PROTOCOL.md CH046_WIRE_COMPRESSION.md \
	ENGINE_IDEAS.md Makefile README.md \
	hat/hatSql/columnar_block_stream.go hat/hatSql/ch046_native_wire_protocol_test.go \
	hat/hatSql/ch046_wire_dictionary_test.go hat/hatSql/ch046_wire_dictionary_benchmark_test.go \
	hat/hatCache/monitoring.go hat/hatCache/sql_columnar_block_http.go \
	hat/hatCache/sql_columnar_block_http_test.go \
	hat/hatCache/ch046_native_wire_protocol_benchmark_test.go \
	scripts/ch046-native-wire-protocol.sh scripts/verify-ch046-native-wire-protocol.sh \
	scripts/stage-ch046-native-wire-protocol.sh scripts/commit-ch046-native-wire-protocol.sh \
	scripts/push-ch046-native-wire-protocol.sh
