#!/bin/sh
set -eu

go test ./hat/hatCache -run 'TestExecuteSQLQuery(ChoosesMostSelectiveIndexedConjunct|FallsBackWhenMostSelectiveIndexIsUnavailable|ExplainAnalyzeReportsIndexCandidateDecision|ExplainAnalyzeReportsIndexEstimateError)|TestSQLAdaptivePlannerSkipsPersistentlyMisleading(Index|IndexedConjunct)' -count=1
go test -race ./hat/hatCache -run 'TestExecuteSQLQuery(ChoosesMostSelectiveIndexedConjunct|FallsBackWhenMostSelectiveIndexIsUnavailable|ExplainAnalyzeReportsIndexCandidateDecision|ExplainAnalyzeReportsIndexEstimateError)|TestSQLAdaptivePlannerSkipsPersistentlyMisleading(Index|IndexedConjunct)' -count=1
go test ./hat/hatSql -run 'TestAdaptivePlanner|TestSQLIndexHint' -count=1
go test -race ./hat/hatSql -run 'TestAdaptivePlanner|TestSQLIndexHint' -count=1
go vet ./hat/hatCache
go vet ./hat/hatSql
