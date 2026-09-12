#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '===== hat/hatSql/subscription.go ====='
sed -n '250,460p' hat/hatSql/subscription.go
printf '%s\n' '===== canonical row helpers ====='
sed -n '1,180p' hat/hatSql/collation.go
sed -n '12340,12390p' hat/hatSql/query.go
