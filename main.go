package main

// note: there should be no newline before import "C", also do not add code above this
// cgo types: https://gist.github.com/zchee/b9c99695463d8902cd33

// #cgo LDFLAGS: -L${SRCDIR} -L. -lhat-trie
// #cgo CFLAGS: -g -Wall
// #include "hat-trie/hat-trie.h"
// extern hattrie_t* hattrie_create (void);
// extern value_t* hattrie_get (hattrie_t*, const char* key, size_t len); // get and set
// extern void       hattrie_free   (hattrie_t*);
// extern value_t* hattrie_tryget (hattrie_t*, const char* key, size_t len);
// extern int hattrie_del(hattrie_t* T, const char* key, size_t len);
import "C"
import (
	"fmt"
	"github.com/kokizzu/gotro/M"
	"github.com/kokizzu/gotro/S"
	"github.com/kokizzu/gotro/X"
	"unsafe"
)

// HatValue stores 5 byte
const DATAVALUE_SIZE_BYTE = 5
const DATAVALUE_TTL_BIT_SHIFT = 8
const DATAVALUE_TTL_TYPE_BITS = 1 << DATAVALUE_TTL_BIT_SHIFT - 1
const DATAVALUE_TTL_OFFSET = 32
const DATAVALUE_VALUE_BITS = 1 << DATAVALUE_TTL_OFFSET - 1
const (
	DATAVALUE_TYPE_NULL uint8 = iota
	DATAVALUE_TYPE_COUNTER
	DATAVALUE_TYPE_RAW_BYTES
	DATAVALUE_TYPE_RAW_STRING
	// TODO: add more types (deque, priority queue, etc)
)

type HatValue struct {
	Index int32 // 32bit index to HatTrie.raws or integer counter
	Type uint8 // 1bit TTL + 7bit TYPE
}

func (hval HatValue) Empty() bool {
	return hval.Type == 0 
}

func (hval HatValue) Is(cmp uint8) bool {
	return hval.Type & DATAVALUE_TTL_TYPE_BITS == cmp
}

func (hval HatValue) IsCounter() bool {
	return hval.Is(DATAVALUE_TYPE_COUNTER)
}

func (hval HatValue) IsBytesAtRaws() bool {
	return hval.Is(DATAVALUE_TYPE_RAW_BYTES)
}

func (hval HatValue) IsStringAtRaws() bool {
	return hval.Is(DATAVALUE_TYPE_RAW_STRING)
}

func (hval HatValue) HasTtl() bool {
	return hval.Type | DATAVALUE_TTL_BIT_SHIFT == 1
}

func (hval HatValue) String() string {
	// hval.HasTtl()
	switch hval.Type & DATAVALUE_TTL_TYPE_BITS {
	case DATAVALUE_TYPE_NULL:
		return `null hat value`
	case DATAVALUE_TYPE_COUNTER:
		return `int32 counter: ` + X.ToS(hval.Index)
	case DATAVALUE_TYPE_RAW_BYTES:
		return `raw SV at index: ` + X.ToS(hval.Index)
	case DATAVALUE_TYPE_RAW_STRING:
		return `string at index: ` + X.ToS(hval.Index)
	}
	return `unknown type`
}

func (hval HatValue) ToUlong() C.ulong {
	return C.ulong(uint64(hval.Type) << DATAVALUE_TTL_OFFSET | uint64(hval.Index))
}

func (hval *HatValue) FromUlong(ulong C.ulong) {
	hval.Type = uint8(ulong >> DATAVALUE_TTL_OFFSET)
	hval.Index = int32(ulong & DATAVALUE_VALUE_BITS)
}

// raw serialized array
type SerializedValues struct {
	array     [][]byte
	reusables map[int]bool
}

func CreateSerializedValues() *SerializedValues {
	return &SerializedValues{
		array:     [][]byte{},
		reusables: map[int]bool{},
	}
}

func (sv *SerializedValues) Put(idx int32, value []byte) {
	sv.array[idx] = value
}

func (sv *SerializedValues) Append(value []byte) int32 {
	sv.array = append(sv.array,value)
	return int32(len(sv.array) - 1)
}

func (sv *SerializedValues) Add(value []byte) int32 {
	if len(sv.reusables) > 0 {
		freeIdx := int32(0)
		for idx := range sv.reusables {
			freeIdx = int32(idx)
			break
		}
		sv.array[freeIdx] = value
		delete(sv.reusables,int(freeIdx))
		return freeIdx
	}
	return sv.Append(value)
}

// HAT-Trie
type HatTrie struct {
	root *C.struct_hattrie_t_
	raws *SerializedValues
}

func CreateHatTrie() *HatTrie {
	return &HatTrie{
		root: C.hattrie_create(), 
		raws: CreateSerializedValues(),
	}
}

func (ht *HatTrie) Destroy() {
	C.hattrie_free(ht.root)
}

func (ht *HatTrie) upsertLocation(key string) *C.ulong {
	cstr := C.CString(key)
	defer C.free(unsafe.Pointer(cstr))
	
	iter := C.hattrie_get(ht.root, cstr, DATAVALUE_SIZE_BYTE)
	return iter
}

func (ht *HatTrie) Get(key string) HatValue {
	cstr := C.CString(key)
	defer C.free(unsafe.Pointer(cstr))
	
	iter := C.hattrie_tryget(ht.root, cstr, DATAVALUE_SIZE_BYTE)
	hval := HatValue{}
	if iter != nil {
		hval.FromUlong(*iter)
	}
	return hval
}

// delete value at key
func (ht *HatTrie) Del(key string) {
	cstr := C.CString(key)
	defer C.free(unsafe.Pointer(cstr))
	
	C.hattrie_del(ht.root, cstr, DATAVALUE_SIZE_BYTE)
}

//// counter type

// set type as counter at key
func (ht *HatTrie) UpsertCounter(key string, val int32) {
	*ht.upsertLocation(key) = HatValue{Index: val, Type: DATAVALUE_TYPE_COUNTER}.ToUlong()
}

// increment counter at key, if type not type of counter, will reset the value to "by"
func (ht *HatTrie) IncrementCounter(key string, by int32) {
	hval := HatValue{}
	rawPtr := ht.upsertLocation(key)
	hval.FromUlong(*rawPtr)
	if hval.IsCounter() {
		hval.Index += by		
	} else {
		hval.Type = DATAVALUE_TYPE_COUNTER
		hval.Index = by
	}
	*rawPtr = hval.ToUlong()
}

// return 0 if type not counter
func (ht *HatTrie) GetCounter(s string) int32 {
	hval := ht.Get(s)
	if hval.IsCounter() {
		return hval.Index
	}
	return 0
}

//// string type

// set type as string/raw index at key
func (ht *HatTrie) UpsertString(key string, val string) {
	idx := ht.raws.Add([]byte(val))
	*ht.upsertLocation(key) = HatValue{Index: idx, Type: DATAVALUE_TYPE_RAW_STRING}.ToUlong()
}

// append string at key, if type not string, will reset the value to "str"
func (ht *HatTrie) AppendString(key string, str string) {
	hval := HatValue{}
	rawPtr := ht.upsertLocation(key)
	hval.FromUlong(*rawPtr)
	if hval.IsStringAtRaws() {
		old := ht.raws.array[hval.Index]
		ht.raws.Put(hval.Index,[]byte(string(old) + str))
	} else {
		hval.Type = DATAVALUE_TYPE_RAW_STRING
		if hval.Empty() {
			hval.Index = ht.raws.Add([]byte(str))
		} else {
			ht.raws.Put(hval.Index,[]byte(str))
		}
		*rawPtr = hval.ToUlong()
	}
}

// prepend string at key, if type not string, will reset the value to "str"
func (ht *HatTrie) PrependString(key string, str string) {
	hval := HatValue{}
	rawPtr := ht.upsertLocation(key)
	hval.FromUlong(*rawPtr)
	if hval.IsStringAtRaws() {
		old := ht.raws.array[hval.Index]
		ht.raws.Put(hval.Index,[]byte(str+string(old)))
	} else {
		hval.Type = DATAVALUE_TYPE_RAW_STRING
		if hval.Empty() {
			hval.Index = ht.raws.Add([]byte(str))
		} else {
			ht.raws.Put(hval.Index,[]byte(str))
		}
		*rawPtr = hval.ToUlong()
	}
}

// return empty string if type not string/counter
func (ht *HatTrie) GetString(key string) string {
	hval := ht.Get(key)
	if hval.IsStringAtRaws() {
		return string(ht.raws.array[hval.Index]) 
	} else if hval.IsCounter() {
		return X.ToS(hval.Index)
	}
	return ``
}

//// bytes (or serialized)

// serialized type (bytes)
func (ht *HatTrie) UpsertBytes(key string, val []byte) {
	idx := ht.raws.Add(val)
	*ht.upsertLocation(key) = HatValue{Index: idx, Type: DATAVALUE_TYPE_RAW_BYTES}.ToUlong()
}

// return nil if type not string/bytes
func (ht *HatTrie) GetBytes(key string) []byte {
	hval := ht.Get(key)
	if hval.IsStringAtRaws() || hval.IsBytesAtRaws() {
		return ht.raws.array[hval.Index] 
	} 
	return nil
}

// TODO: add sync to each operation, because all operation not thread safe

func main() {
	// create and destroy
	h := CreateHatTrie()
	defer h.Destroy()
	
	// counter test
	fmt.Println(`
-- counter test`)
	const key1= `test`
	h.UpsertCounter(key1, 5) // upsert
	fmt.Println(h.Get(key1)) // get raw hatValue
	fmt.Println(h.GetString(key1) + ` get as string`)
	h.IncrementCounter(key1, -2)    // increment
	fmt.Println(h.GetCounter(key1)) // get
	h.Del(key1)                     // delete	
	fmt.Println(h.Get(key1))        // check if deleted
	
	// string test
	fmt.Println(`
-- string test`)
	const key2= `foo`
	h.UpsertString(key2, `eat`) // set
	fmt.Println(h.GetString(key2))
	h.AppendString(key2, ` nasi`) // append
	fmt.Println(h.GetString(key2))
	h.PrependString(key2, `i `) // prepend
	fmt.Println(h.GetString(key2))
	h.Del(key2)                            // delete
	h.AppendString(key2, `mie ayam jamur`) // append again
	fmt.Println(h.GetString(key2))
	h.Del(key2)                        // delete
	h.PrependString(key2, `pizza hut`) // prepend again
	fmt.Println(h.GetString(key2))
	
	// serialized value test (without flatbuffer/fastbinaryencoding)
	fmt.Println(`
-- bytes test`)
	p1 := M.SX{`name`: `koki`, `age`: 32}
	const key3= `test3`
	h.UpsertBytes(key3, []byte(X.ToJson(p1)))   // upsert
	p2 := S.JsonToMap(string(h.GetBytes(key3))) // get
	fmt.Println(p2)
}