#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestKeyWatcher(PrefixFilter|CoalescesLatestEventPerKey|CoalescesPrefixKeysInFirstSeenOrder|CoalescedClosesWhenTrieIsDestroyed|OptionsRejectAmbiguousFilters)$' -count=1
