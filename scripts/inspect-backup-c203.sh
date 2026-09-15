#!/usr/bin/env bash
set -euo pipefail

printf 'Backup package files:\n'
rg --files hat/hatBackup | sort
printf '\nManifest model:\n'
sed -n '1,95p' hat/hatBackup/model.go
printf '\nChain validation model:\n'
sed -n '1,180p' hat/hatBackup/chain.go
sed -n '220,315p' hat/hatBackup/chain.go
printf '\nExported backup symbols:\n'
rg -n '^// |^(type|func) ' hat/hatBackup --glob '*.go' | head -n 240
printf '\nBackup tests:\n'
rg --files hat/hatBackup --glob '*_test.go' | sort
