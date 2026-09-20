#!/usr/bin/env bash
set -euo pipefail

rg -n -C 2 'C248|C050|CH031|CH044|JSON_SUBCOLUMNS' \
  INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md
sed -n '1,220p' JSON_SUBCOLUMNS.md
sed -n '1,220p' CH031_TYPED_JSON_SUBCOLUMNS.md
sed -n '1,220p' CH044_JSON_DYNAMIC_SUBCOLUMNS.md
sed -n '1,260p' hat/hatSql/columnar_json_subcolumn.go
sed -n '1,260p' hat/hatCache/sql_json_subcolumn.go
rg -n 'test-ch031|benchmark-ch031|test-ch044|benchmark-ch044|json-subcolumns|typed-json-subcolumns|dynamic-json-subcolumns' \
  Makefile scripts --glob '*.sh' --glob 'Makefile'
