#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
printf '%s\n' '== command dispatch declarations =='
rg -n '^\s*case "[A-Z0-9_]+' "$root/hat/hatCache/command.go"
printf '%s\n' '== existing concurrent and race tests =='
rg -n -l 'Concurrent|concurrent|Race|race\.Enabled|WaitGroup' "$root" --glob '*_test.go'
printf '%s\n' '== mutable package files =='
rg -n -l 'sync\.(Mutex|RWMutex)|atomic\.' "$root/hat/hatCache" --glob '*.go' --glob '!**/*_test.go'
