package hatCache

import (
	"strconv"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var _ hatSql.BorrowedIndexedSourceResolver = (*HatTrie)(nil)

func TestBorrowSQLIndexedSourceRefreshesImmutablePostings(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"team":"blue","name":"ocean"}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "team"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	first, available, err := trie.BorrowSQLIndexedSource("CACHE", "people", "team", "blue")
	if err != nil || !available || len(first) != 1 || first[0]["name"] != "ocean" {
		t.Fatalf("first borrowed postings = %#v, available %t, error %v", first, available, err)
	}

	trie.UpsertString("people", `[{"team":"red","name":"fire"}]`)
	second, available, err := trie.BorrowSQLIndexedSource("CACHE", "people", "team", "red")
	if err != nil || !available || len(second) != 1 || second[0]["name"] != "fire" {
		t.Fatalf("refreshed borrowed postings = %#v, available %t, error %v", second, available, err)
	}
	if first[0]["name"] != "ocean" {
		t.Fatalf("previous borrowed snapshot was mutated: %#v", first)
	}
}

func TestSQLBorrowedIndexJoinKeepsHashPlanForHotDimension(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	var facts strings.Builder
	facts.WriteByte('[')
	for row := 0; row < 100; row++ {
		if row > 0 {
			facts.WriteByte(',')
		}
		facts.WriteString(`{"id":`)
		facts.WriteString(strconv.Itoa(row))
		facts.WriteString(`,"team":"blue"}`)
	}
	facts.WriteByte(']')
	trie.UpsertString("facts", facts.String())
	trie.UpsertString("dimensions", `[{"team":"blue","name":"ocean"}]`)
	if err := trie.CreateSQLJSONFieldIndex("dimensions", "team"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	result, err := ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('facts') AS fact JOIN CACHE('dimensions') AS dimension ON fact.team = dimension.team SELECT fact.id, dimension.name", trie)
	if err != nil {
		t.Fatalf("indexed hot-dimension join = %#v, error %v", result, err)
	}
	for _, step := range result.Plan {
		if step.Node == "HASH JOIN" {
			return
		}
	}
	t.Fatalf("plan = %#v, want HASH JOIN", result.Plan)
}
