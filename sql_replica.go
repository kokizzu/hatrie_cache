package hatriecache

import "hatrie_cache/hat/hatSql"

// SQLReadReplicaSet is the importable hatSql read router retained under the
// root API for compatibility with existing callers.
type SQLReadReplicaSet = hatSql.ReadReplicaSet

// NewSQLReadReplicaSet creates a search-read router from one or more SQL
// source resolvers. *HatTrie values satisfy this interface directly.
func NewSQLReadReplicaSet(replicas ...SQLSourceResolver) (*SQLReadReplicaSet, error) {
	return hatSql.NewReadReplicaSet(replicas...)
}
