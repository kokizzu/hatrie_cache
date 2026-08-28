#!/usr/bin/env sh
set -eu

go test ./hat/hatStorage -run '^TestNamespaceLifecycle'
