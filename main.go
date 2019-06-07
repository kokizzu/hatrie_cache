package main
import "C"
import (
	"fmt"
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
	
	key := C.CString("Gopher")
	defer C.free(unsafe.Pointer(key))
	
	// create and destroy
	h := C.hattrie_create()
	defer C.hattrie_free(h)
	
	// insert or update
	v := C.hattrie_get(h, key, 8)
	*v = 123
	
	// get
	r := C.hattrie_tryget(h, key, 8)
	if r != nil {
		fmt.Println(`the value is `, *r)
	}
	
	// delete
	C.hattrie_del(h,key,8)
	
	// check if deleted
	r =  C.hattrie_tryget(h, key, 8)
	if r != nil {
		fmt.Println(*r)
	} else {
		fmt.Println(`deleted or not exists`)
	}
}