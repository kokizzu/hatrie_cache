#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

printf '== function contracts and evaluation ==\n'
rg -n -A 100 'type (FunctionDefinition|FunctionCall|FunctionResolver|FunctionRuntime)|func .*Function' "$root/hat/hatSql" --glob '*.go'

printf '\n== JSON expression and index paths ==\n'
rg -n -A 80 -i 'json.*(path|index|extract)|extract.*json|json_extract|json_field' "$root/hat/hatSql" "$root/hat/hatCache/sql_query.go" --glob '*.go'
