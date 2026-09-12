#!/bin/sh
set -eu

printf '%s\n' 'Open or partially implemented inspiration entries:'
rg -n '^- \[[^xX]\]' INSPIRATION.md || true
