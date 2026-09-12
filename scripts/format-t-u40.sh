#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
gofmt -w hat/hatDataStructure/tuple_format_negotiation.go hat/hatDataStructure/tuple_format_negotiation_test.go
