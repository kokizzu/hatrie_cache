#!/bin/sh
set -eu

go test ./hat/hatAuth -run '^TestIdentityChainUsesTokenOIDCAndTrustedProxyHeader$'
