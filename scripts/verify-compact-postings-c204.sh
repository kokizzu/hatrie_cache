#!/usr/bin/env bash
set -euo pipefail

pattern='Test(FunctionalIndex|HashIndex|StringMultikeyIndex|ConditionalFunctionalIndex|U64PostingList)'
go test ./hat/hatDataStructure -run "$pattern" -count=1
go test ./hat/hatDataStructure -run "$pattern" -race -count=1
go vet ./hat/hatDataStructure
