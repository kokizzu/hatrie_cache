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
	"github.com/kokizzu/gotro/A"
	"github.com/kokizzu/gotro/M"
	"github.com/kokizzu/gotro/S"
	"github.com/kokizzu/gotro/X"
	"unsafe"
)

/////////////
// HatValue: 5 bytes that saved on the hat_trie
type HatValue struct {
	Index int32 // 32bit index to HatTrie.raws or integer counter
	Type uint8 // 1bit TTL + 7bit TYPE
}

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
	DATAVALUE_TYPE_MAP
	DATAVALUE_TYPE_SLICE
	// TODO: add more types (deque, priority queue, etc)
)

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

func (hval HatValue) IsRaw() bool {
	return hval.IsStringAtRaws() || hval.IsBytesAtRaws()
}

func (hval HatValue) IsMap() bool {
	return hval.Is(DATAVALUE_TYPE_MAP)
}

func (hval HatValue) IsSlice() bool {
	return hval.Is(DATAVALUE_TYPE_SLICE)
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
	//case DATAVALUE_TYPE_MAP:
	//	return `map at index: ` + X.ToS(hval.Index)
	//case DATAVALUE_TYPE_SLICE:
	//	return `slice at index: ` + X.ToS(hval.Index)
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

/////////////
// BytesStorage, for string and bytes/serialized values

type BytesStorage struct {
	array     [][]byte
	reusables map[int32]bool
}

func CreateBytesStorage() *BytesStorage {
	return &BytesStorage{
		array:     [][]byte{},
		reusables: map[int32]bool{},
	}
}

func (bs *BytesStorage) Put(idx int32, value []byte) {
	bs.array[idx] = value
}

func (bs *BytesStorage) Append(value []byte) int32 {
	bs.array = append(bs.array,value)
	return int32(len(bs.array) - 1)
}

func (bs *BytesStorage) Add(value []byte) int32 {
	if len(bs.reusables) > 0 {
		freeIdx := int32(0)
		for idx := range bs.reusables {
			freeIdx = int32(idx)
			break
		}
		bs.array[freeIdx] = value
		delete(bs.reusables,freeIdx)
		return freeIdx
	}
	return bs.Append(value)
}

func (bs *BytesStorage) Del(idx int32) {
	bs.array[idx] = nil
	bs.reusables[idx] = true
}

/////////////
// MapStorage
type MapStorage struct {
	array     []M.SX
	reusables map[int32]bool
}

func CreateMapStorage() *MapStorage{
	return &MapStorage{
		array:     []M.SX{},
		reusables: map[int32]bool{},
	}
}

func (ms *MapStorage) Put(idx int32, value M.SX) {
	ms.array[idx] = value
}

func (ms *MapStorage) Append(value M.SX) int32 {
	ms.array = append(ms.array,value)
	return int32(len(ms.array) - 1)
}

func (ms *MapStorage) Add(value M.SX) int32 {
	if len(ms.reusables) > 0 {
		freeIdx := int32(0)
		for idx := range ms.reusables {
			freeIdx = int32(idx)
			break
		}
		ms.array[freeIdx] = value
		delete(ms.reusables,freeIdx)
		return freeIdx
	}
	return ms.Append(value)
}

func (ms *MapStorage) Del(idx int32) {
	ms.array[idx] = nil
	ms.reusables[idx] = true
}

/////////////
// SliceStorage
type SliceStorage struct {
	array     []A.X
	reusables map[int32]bool
}

func CreateSliceStorage() *SliceStorage{
	return &SliceStorage{
		array:     []A.X{},
		reusables: map[int32]bool{},
	}
}

func (ss *SliceStorage) Put(idx int32, value A.X) {
	ss.array[idx] = value
}

func (ss *SliceStorage) Append(value A.X) int32 {
	ss.array = append(ss.array,value)
	return int32(len(ss.array) - 1)
}

func (ss *SliceStorage) Add(value A.X) int32 {
	if len(ss.reusables) > 0 {
		freeIdx := int32(0)
		for idx := range ss.reusables {
			freeIdx = int32(idx)
			break
		}
		ss.array[freeIdx] = value
		delete(ss.reusables,freeIdx)
		return freeIdx
	}
	return ss.Append(value)
}

func (ss *SliceStorage) Del(idx int32) {
	ss.array[idx] = nil
	ss.reusables[idx] = true
}

/////////////
// HAT-Trie
type HatTrie struct {
	root *C.struct_hattrie_t_
	raws *BytesStorage
	maps *MapStorage
	slices *SliceStorage
}

func CreateHatTrie() *HatTrie {
	return &HatTrie{
		root: C.hattrie_create(), 
		raws: CreateBytesStorage(),
		maps: CreateMapStorage(),
		slices: CreateSliceStorage(),
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

func (ht *HatTrie) returnStorage(hval HatValue) {
	if hval.Empty() {
		return
	}
	if hval.IsMap() {
		ht.maps.Del(hval.Index)
	} else if hval.IsSlice() {
		ht.slices.Del(hval.Index)		
	} else if hval.IsRaw() {
		ht.raws.Del(hval.Index)		
	}
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

/////////////
//// counter type operations

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

/////////////
//// string type operations

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
		ht.returnStorage(hval)
		hval.Index = ht.raws.Add([]byte(str))
		hval.Type = DATAVALUE_TYPE_RAW_STRING
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
		ht.returnStorage(hval)
		hval.Index = ht.raws.Add([]byte(str))
		hval.Type = DATAVALUE_TYPE_RAW_STRING
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

/////////////
//// bytes (or serialized) operations

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

/////////////
//// maps operations

func (ht *HatTrie) UpsertMap(key string, val M.SX) {
	idx := ht.maps.Add(val)
	*ht.upsertLocation(key) = HatValue{Index: idx, Type: DATAVALUE_TYPE_MAP}.ToUlong()
}

func (ht *HatTrie) PutMap(key string, subkey string, val interface{}) {
	hval := HatValue{}
	rawPtr := ht.upsertLocation(key)
	hval.FromUlong(*rawPtr)
	if hval.IsMap() {
		old := ht.maps.array[hval.Index]
		old[subkey] = val
	} else {
		ht.returnStorage(hval)
		hval.Index = ht.maps.Add(M.SX{key:val})
		hval.Type = DATAVALUE_TYPE_RAW_BYTES
		*rawPtr = hval.ToUlong()
	}	
}

func (ht *HatTrie) PeekMap(key, subkey string) interface{} {
	m := ht.GetMap(key) 
	if m != nil {
		return m[subkey]
	}
	return nil
}

func (ht *HatTrie) TakeMap(key, subkey string) interface{} {
	m := ht.GetMap(key) 
	if m != nil {
		val := m[subkey]
		delete(m,subkey)
		return val
	}
	return nil
}

func (ht *HatTrie) GetMap(key string) M.SX {
	hval := ht.Get(key)
	if hval.IsMap() {
		return ht.maps.array[hval.Index]
	}
	return nil
}

/////////////
//// slices 

func (ht *HatTrie) UpsertSlice(key string, val A.X) {
	idx := ht.slices.Add(val)
	*ht.upsertLocation(key) = HatValue{Index: idx, Type: DATAVALUE_TYPE_SLICE}.ToUlong()
}

func (ht *HatTrie) PushSlice(key string, val interface{}, vals... interface{}) {
	hval := HatValue{}
	rawPtr := ht.upsertLocation(key)
	hval.FromUlong(*rawPtr)
	if hval.IsSlice() {
		old := ht.slices.array[hval.Index]
		old = append(old,val)
		if len(vals) > 0 {
			old = append(old,vals...)
		}
		ht.slices.Put(hval.Index,old)
	} else {
		ht.returnStorage(hval)
		arr := A.X{val}
		if len(vals) > 0 {
			arr = append(arr,vals...)
		}		
		hval.Index = ht.slices.Add(arr)
		hval.Type = DATAVALUE_TYPE_RAW_STRING
		*rawPtr = hval.ToUlong()
	}	
}

func (ht *HatTrie) PopSlice(key string) interface{} {
	hval := HatValue{}
	rawPtr := ht.upsertLocation(key)
	hval.FromUlong(*rawPtr)
	if hval.IsSlice() {
		old := ht.slices.array[hval.Index]
		last := len(old) - 1
		if last > -1 {
			val := old[last]
			old = old[:last]
			ht.slices.Put(hval.Index,old)
			return val 
		}
	}
	return nil
}

func (ht *HatTrie) ShiftSlice(key string) interface{} {
	hval := HatValue{}
	rawPtr := ht.upsertLocation(key)
	hval.FromUlong(*rawPtr)
	if hval.IsSlice() {
		old := ht.slices.array[hval.Index]
		if len(old) > 0 {
			val := old[0]
			old = old[1:]
			ht.slices.Put(hval.Index,old)
			return val 
		}
	} 
	return nil
}

func (ht *HatTrie) HeadSlice(key string) interface{} {
	sl := ht.GetSlice(key)
	if sl != nil {
		if len(sl) > 0 {
			return sl[0]
		}
	}
	return nil
}

func (ht *HatTrie) TailSlice(key string) interface{} {
	sl := ht.GetSlice(key)
	if sl != nil {
		last := len(sl)-1
		if last > -1 {
			return sl[last]
		}
	}
	return nil
}

func (ht *HatTrie) GetSlice(key string) A.X {
	hval := ht.Get(key)
	if hval.IsSlice() {
		return ht.slices.array[hval.Index]
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
	
	// map test
	fmt.Println(`
-- map test`)
	const key4 = `tes`
	h.UpsertMap(key4, M.SX{`test`:123}) // set
	fmt.Println(h.GetMap(key4))
	h.PutMap(key4, `name`, `ivi`) // put
	fmt.Println(h.GetMap(key4))
	v := h.PeekMap(key4,`test`) // peek
	fmt.Println(v)
	v = h.TakeMap(key4,`test`) // take
	fmt.Println(h.GetMap(key4))
	
	// slice test
	fmt.Println(`
-- slice test`)
	const key5 = `slicetoy`
	h.UpsertSlice(key5, A.X{1,2,`whoa`})
	fmt.Println(h.GetSlice(key5))
	h.PushSlice(key5,`nyaa`, 4, 5) // push
	fmt.Println(h.GetSlice(key5))
	v = h.ShiftSlice(key5) // shift
	fmt.Println(v)
	fmt.Println(h.GetSlice(key5))
	v = h.PopSlice(key5) // pop
	fmt.Println(v)
	fmt.Println(h.GetSlice(key5))
	v = h.HeadSlice(key5) // front
	fmt.Println(v)
	fmt.Println(h.GetSlice(key5))
	v = h.TailSlice(key5) // tail
	fmt.Println(v)
	fmt.Println(h.GetSlice(key5))

}