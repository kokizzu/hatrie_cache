#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache ./cmd/hatrie-cache -run 'Test(PersistentStoreMaxBytes|ParseConfigSeparatesCacheAndStorageSizeLimits|ParseConfigLegacyDBMemoryCapFeedsCacheLimit)' -count=1
