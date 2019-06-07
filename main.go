package main
import "C"
import (
	"fmt"
	"unsafe"
)

// note: there should be no newline before import "C" 

// current steps:
// gcc -std=c++17 -c -g global_hat_trie.cc -static -static-libgcc -static-libstdc++ && mv global_hat_trie.o libglobal_hat_trie.a 
// go build -x main.go


// #cgo LDFLAGS: -lstdc++ -L${SRCDIR} -L. -lglobal_hat_trie
// #cgo CFLAGS: -g -Wall
// #cgo CXXFLAGS: -std=c++17
// #include <stdlib.h>
// #include "global_hat_trie.h"
import "C"
import (
	"fmt"
	"unsafe"
)

func main() {
	
	// TODO: https://karthikkaranth.me/blog/calling-c-code-from-go/
	key := C.CString("Gopher")
	defer C.free(unsafe.Pointer(key))
	
	fmt.Println(C.HatInsert(key,1))
	
	fmt.Println(C.HatFind(key))
	
	fmt.Println(C.HatErase(key))
	
	fmt.Println(C.HatFind(key))
	
}