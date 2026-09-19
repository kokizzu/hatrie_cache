#!/bin/sh
set -eu

gofmt -w \
  hat/hatSql/m_u05_arrangement_recovery.go \
  hat/hatSql/m_u05_arrangement_recovery_test.go
