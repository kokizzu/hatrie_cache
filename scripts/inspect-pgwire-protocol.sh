#!/usr/bin/env sh
set -eu

printf '%s\n' '== metrics and pgwire instrumentation surface =='
rg --files hat/hatMetrics hat/hatPgWire
rg -n -A35 -B8 'type .*Metrics|Metrics' hat/hatMetrics hat/hatPgWire
printf '%s\n' '== pgwire formatter =='
sed -n '1,160p' scripts/format-pgwire.sh
