#include "global_hat_trie.h"
#include "tessil__hat_trie/include/tsl/htrie_map.h"
#include <stdint.h>
#include <utility>

// compile with: g++ -c global_hat_trie.cpp

// global kv
tsl::htrie_map<char,int64_t> global_htrie;

int HatInsert(const char* key, const int64_t value) {
	auto iter_bool = global_htrie.insert(key, value);
	return std::get<1>(iter_bool);
}

int HatErase(const char* key) {
	return global_htrie.erase(key);
}

int64_t HatFind(const char* key) {
	auto iter = global_htrie.find(key);
	if(iter != global_htrie.end()) return *iter;
	return 0;
}