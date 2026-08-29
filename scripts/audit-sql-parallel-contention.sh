#!/usr/bin/env sh
set -eu

grep -R -n -m 600 -E 'func sqlMerge|Merge.*Run|sync\.(Mutex|RWMutex)|Lock\(|RLock\(|Workers|worker' hat/hatSql hat/hatCache
