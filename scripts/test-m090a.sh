#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run 'TestSQL(ResolverAdapter|NamespaceAdapter|AdapterRegistry)' -count=1
