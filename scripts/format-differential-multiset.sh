#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
gofmt -w hat/hatDataStructure/differential_multiset.go hat/hatDataStructure/differential_multiset_test.go
