#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -run 'TestSQL(ResolverAdapter|NamespaceAdapter|AdapterRegistry)' -count=1
