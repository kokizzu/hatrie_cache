#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatExtension/native_extension.go \
	hat/hatExtension/native_extension_test.go \
	hat/hatExtension/native_extension_baseline_test.go \
	hat/hatExtension/native_extension_example_test.go
