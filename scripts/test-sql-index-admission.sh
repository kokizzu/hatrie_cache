#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLJSONIndexAdmissionBudget' -count=1
