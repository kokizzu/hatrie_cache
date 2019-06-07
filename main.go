package main
import "C"
import (
	"unsafe"
)

// note: there should be no newline before import "C"

// #cgo LDFLAGS: -L${SRCDIR} -L. -lhat-trie
// #cgo CFLAGS: -g -Wall
// #include "hat-trie/hat-trie.h"
// extern hattrie_t* hattrie_create (void);
// extern value_t* hattrie_get (hattrie_t*, const char* key, size_t len); // get and set
// extern void       hattrie_free   (hattrie_t*);
// extern value_t* hattrie_tryget (hattrie_t*, const char* key, size_t len);
// extern int hattrie_del(hattrie_t* T, const char* key, size_t len);
import "C"

func main() {
	
	// TODO: https://karthikkaranth.me/blog/calling-c-code-from-go/
	key := C.CString("Gopher")
	defer C.free(unsafe.Pointer(key))
	
	h := C.hattrie_create()
	v := C.hattrie_get(h, key, 8)
	*v = 1
	
	
}