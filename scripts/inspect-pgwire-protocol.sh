#!/usr/bin/env sh
set -eu

printf '%s\n' '== pgwire delivery scope =='
sed -n '1,220p' scripts/deliver-pgwire-extended.sh
