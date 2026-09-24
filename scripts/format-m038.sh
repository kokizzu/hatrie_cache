#!/bin/sh
set -eu

gofmt -w hat/hatMerkle/part_catalog_persistence.go hat/hatMerkle/m_u38_persisted_part_catalog_test.go hat/hatMerkle/m_u38_persisted_part_catalog_benchmark_test.go
