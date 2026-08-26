package hatCache

import "embed"

// nativeBuildInputs makes Go's build cache track sources included by hattrie_cgo.c.
//
//go:embed luikore__hat-trie/src/*.c luikore__hat-trie/src/*.h
var nativeBuildInputs embed.FS
