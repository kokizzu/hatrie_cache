package hatriecache

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	json "github.com/goccy/go-json"
)

const maxCommandTTLSeconds = int64(1<<63-1) / int64(time.Second)
const maxPublicCommandBatchSize = 4096

type CacheCommandRequest struct {
	Command     string                `json:"command"`
	Key         string                `json:"key"`
	Value       string                `json:"value,omitempty"`
	Values      Slice                 `json:"values,omitempty"`
	Batch       []CacheCommandRequest `json:"batch,omitempty"`
	Subkey      string                `json:"subkey,omitempty"`
	Pairs       Map                   `json:"pairs,omitempty"`
	Priority    *int64                `json:"priority,omitempty"`
	TTLSeconds  *int64                `json:"ttl_seconds,omitempty"`
	UnixSeconds *int64                `json:"unix_seconds,omitempty"`
	BinaryValue []byte                `json:"-"`
}

type CacheCommandResponse struct {
	OK        bool                   `json:"ok"`
	Message   string                 `json:"message"`
	Value     string                 `json:"value,omitempty"`
	Responses []CacheCommandResponse `json:"responses,omitempty"`
}

func (ht *HatTrie) ExecuteCommand(request CacheCommandRequest) CacheCommandResponse {
	if ht == nil {
		return commandError(ErrNilHatTrie.Error())
	}
	if ht.localPartitionSet() != nil {
		command := strings.ToUpper(strings.TrimSpace(request.Command))
		if command == "BATCH" {
			return ht.executePartitionedPublicBatchCommand(request)
		}
		if key := strings.TrimSpace(request.Key); key != "" {
			return ht.localPartitionForKey(key).ExecuteCommand(request)
		}
	}
	if response, ok := ht.executeExactFastCommand(request); ok {
		return response
	}
	command := strings.ToUpper(strings.TrimSpace(request.Command))
	key := strings.TrimSpace(request.Key)
	if command == "" {
		return commandError("command is required")
	}
	if command == "BATCH" {
		return ht.executePublicBatchCommand(request)
	}
	if key == "" {
		return commandError("key is required")
	}
	if err := validateKey(key); err != nil {
		return commandError(err.Error())
	}

	switch command {
	case "GET", "GETSTR":
		value, ok, err := ht.commandValue(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "key not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: value}
	case "DUMP":
		value, ok, err := ht.commandDumpEntry(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "key not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: value}
	case "EXISTS":
		if !ht.Exists(key) {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
	case "SET", "SETSTR":
		if response, ok := validateOptionalCommandExpiration(request.TTLSeconds, request.UnixSeconds); ok && !response.OK {
			return response
		}
		if err := ht.UpsertStringChecked(key, request.Value); err != nil {
			return commandError(err.Error())
		}
		if response, ok := ht.applyCommandExpiration(key, request.TTLSeconds, request.UnixSeconds); ok {
			return response
		}
		return CacheCommandResponse{OK: true, Message: "stored string"}
	case "SETX", "SETSTRX":
		ttl, ok := requirePositiveTTL(request.TTLSeconds)
		if !ok {
			return commandError("positive ttl_seconds is required")
		}
		if err := ht.UpsertStringChecked(key, request.Value); err != nil {
			return commandError(err.Error())
		}
		if !ht.Expire(key, ttl) {
			return commandError("failed to set ttl")
		}
		return CacheCommandResponse{OK: true, Message: "stored string with ttl"}
	case "SETINT":
		value, ok := parseCommandInt32(request.Value)
		if !ok {
			return commandError("value must be a 32-bit integer")
		}
		if response, ok := validateOptionalCommandExpiration(request.TTLSeconds, request.UnixSeconds); ok && !response.OK {
			return response
		}
		if err := ht.UpsertCounterChecked(key, value); err != nil {
			return commandError(err.Error())
		}
		if response, ok := ht.applyCommandExpiration(key, request.TTLSeconds, request.UnixSeconds); ok {
			return response
		}
		return CacheCommandResponse{OK: true, Message: "stored counter"}
	case "SETINTX":
		value, ok := parseCommandInt32(request.Value)
		if !ok {
			return commandError("value must be a 32-bit integer")
		}
		ttl, ok := requirePositiveTTL(request.TTLSeconds)
		if !ok {
			return commandError("positive ttl_seconds is required")
		}
		if err := ht.UpsertCounterChecked(key, value); err != nil {
			return commandError(err.Error())
		}
		if !ht.Expire(key, ttl) {
			return commandError("failed to set ttl")
		}
		return CacheCommandResponse{OK: true, Message: "stored counter with ttl"}
	case "INC":
		by, ok := parseCommandIncrement(request.Value)
		if !ok {
			return commandError("value must be a 32-bit integer")
		}
		value, ok, err := ht.commandIncrementCounter(key, by)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return commandError("counter overflow")
		}
		return CacheCommandResponse{OK: true, Message: "incremented", Value: strconv.FormatInt(int64(value), 10)}
	case "DEL":
		if ht.Delete(key) {
			return CacheCommandResponse{OK: true, Message: "deleted"}
		}
		return CacheCommandResponse{OK: true, Message: "key not found"}
	case "INTERNALSET":
		if err := ht.commandInternalSet(key, request.Value); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "internal value stored"}
	case "INTERNALDEL":
		if ht.Delete(key) {
			return CacheCommandResponse{OK: true, Message: "internal value deleted"}
		}
		return CacheCommandResponse{OK: true, Message: "key not found"}
	case "TTL":
		ttl := ht.TTL(key)
		if ttl == NoTTL {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "-1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatInt(int64(ttl/time.Second), 10)}
	case "EXPIRE":
		ttl, ok := requirePositiveTTL(request.TTLSeconds)
		if !ok {
			return commandError("positive ttl_seconds is required")
		}
		if !ht.Expire(key, ttl) {
			return CacheCommandResponse{OK: true, Message: "key not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ttl updated"}
	case "EXPIREAT":
		expiresAt, ok := commandExpireAt(request)
		if !ok {
			return commandError("unix_seconds or integer value is required")
		}
		if !ht.ExpireAt(key, expiresAt) {
			return CacheCommandResponse{OK: true, Message: "key not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ttl updated"}
	case "PUTMAP":
		fields, ok := commandMapFields(request)
		if !ok {
			return commandError("subkey/value or pairs is required")
		}
		if err := ht.commandPutMap(key, fields); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "stored map fields"}
	case "PEEKMAP":
		subkey := strings.TrimSpace(request.Subkey)
		if subkey == "" {
			return commandError("subkey is required")
		}
		value, ok, err := ht.PeekMapChecked(key, subkey)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "TAKEMAP":
		subkey := strings.TrimSpace(request.Subkey)
		if subkey == "" {
			return commandError("subkey is required")
		}
		value, ok, err := ht.TakeMapChecked(key, subkey)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("removed", value)
	case "PUSHSLICE":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		if err := ht.PushSliceChecked(key, values[0], values[1:]...); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "pushed slice values"}
	case "POPSLICE":
		value, ok, err := ht.PopSliceChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("removed", value)
	case "SHIFTSLICE":
		value, ok, err := ht.ShiftSliceChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("removed", value)
	case "HEADSLICE":
		value, ok, err := ht.HeadSliceChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "TAILSLICE":
		value, ok, err := ht.TailSliceChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "ADDSET":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		added, err := ht.AddSetChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "added set values", Value: strconv.Itoa(added)}
	case "REMSET":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		removed, err := ht.RemoveSetChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "removed set values", Value: strconv.Itoa(removed)}
	case "HASSET":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		hit, err := ht.HasSetChecked(key, values[0])
		if err != nil {
			return commandError(err.Error())
		}
		if hit {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "GETSET":
		value, ok, err := ht.GetSetChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "PUSHPQ", "PUSHPRIORITY":
		priority, ok := commandPriority(request)
		if !ok {
			return commandError("priority is required")
		}
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		added, err := ht.PushPriorityQueueChecked(key, priority, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "pushed priority queue values", Value: strconv.Itoa(added)}
	case "PEEKPQ", "PEEKPRIORITY":
		value, ok, err := ht.PeekPriorityQueueChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "POPPQ", "POPPRIORITY":
		value, ok, err := ht.PopPriorityQueueChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("removed", value)
	case "GETPQ", "GETPRIORITY":
		value, ok, err := ht.GetPriorityQueueChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "CREATEBF", "RESERVEBF", "BFRESERVE":
		expectedItems, falsePositiveRate, err := commandBloomFilterConfig(request)
		if err != nil {
			return commandError(err.Error())
		}
		if err := ht.UpsertBloomFilter(key, expectedItems, falsePositiveRate); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created bloom filter"}
	case "ADDBF", "BFADD":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		added, err := ht.AddBloomFilterChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "added bloom filter values", Value: strconv.Itoa(added)}
	case "HASBF", "BFHAS", "BFEXISTS":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		hit, err := ht.HasBloomFilterChecked(key, values[0])
		if err != nil {
			return commandError(err.Error())
		}
		if hit {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "INFOBF", "BFINFO":
		info, ok, err := ht.BloomFilterInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATECF", "RESERVECF", "CFRESERVE":
		capacity, falsePositiveRate, err := commandCuckooFilterConfig(request)
		if err != nil {
			return commandError(err.Error())
		}
		if err := ht.UpsertCuckooFilter(key, capacity, falsePositiveRate); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created cuckoo filter"}
	case "ADDCF", "CFADD":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		added, err := ht.AddCuckooFilterChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "added cuckoo filter values", Value: strconv.Itoa(added)}
	case "HASCF", "CFHAS", "CFEXISTS":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		hit, err := ht.HasCuckooFilterChecked(key, values[0])
		if err != nil {
			return commandError(err.Error())
		}
		if hit {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "DELCF", "REMCF", "CFDEL":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		deleted, err := ht.DeleteCuckooFilterChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "removed cuckoo filter values", Value: strconv.Itoa(deleted)}
	case "INFOCF", "CFINFO":
		info, ok, err := ht.CuckooFilterInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATEXF", "RESERVEXF", "XFRESERVE", "CREATEXOR":
		expectedItems, err := commandXorFilterExpectedItems(request)
		if err != nil {
			return commandError(err.Error())
		}
		if err := ht.UpsertXorFilter(key, expectedItems); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created xor filter"}
	case "ADDXF", "XFADD":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		added, err := ht.AddXorFilterChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "staged xor filter values", Value: strconv.Itoa(added)}
	case "BUILDXF", "XFBUILD":
		info, ok, err := ht.BuildXorFilter(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("built xor filter", info)
	case "HASXF", "XFHAS", "XFEXISTS":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		info, ok, err := ht.XorFilterInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		if !info.Built {
			return commandError("xor filter is not built")
		}
		hit, queryable, err := ht.HasXorFilterChecked(key, values[0])
		if err != nil {
			return commandError(err.Error())
		}
		if !queryable {
			return commandError("xor filter is not built")
		}
		if hit {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "INFOXF", "XFINFO":
		info, ok, err := ht.XorFilterInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATERB", "CREATEROARING", "RBRESERVE":
		if err := ht.UpsertRoaringBitmapChecked(key); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created roaring bitmap"}
	case "ADDRB", "RBADD":
		values, err := roaringBitmapValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		added, err := ht.AddRoaringBitmapChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "added roaring bitmap values", Value: strconv.Itoa(added)}
	case "REMRB", "DELRB", "RBREM", "RBDEL":
		values, err := roaringBitmapValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		removed, err := ht.RemoveRoaringBitmapChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "removed roaring bitmap values", Value: strconv.Itoa(removed)}
	case "HASRB", "RBHAS", "RBEXISTS":
		values, err := roaringBitmapValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		hit, err := ht.HasRoaringBitmapChecked(key, values[0])
		if err != nil {
			return commandError(err.Error())
		}
		if hit {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "COUNTRB", "RBCOUNT":
		count, ok, err := ht.CountRoaringBitmapChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatUint(count, 10)}
	case "GETRB", "RBGET":
		values, ok, err := ht.GetRoaringBitmapChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", values)
	case "INFORB", "RBINFO":
		info, ok, err := ht.RoaringBitmapInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATESB", "CREATESPARSEBITSET", "SBRESERVE":
		if err := ht.UpsertSparseBitsetChecked(key); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created sparse bitset"}
	case "ADDSB", "SBADD":
		values, err := sparseBitsetValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		added, err := ht.AddSparseBitsetChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "added sparse bitset values", Value: strconv.Itoa(added)}
	case "REMSB", "DELSB", "SBREM", "SBDEL":
		values, err := sparseBitsetValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		removed, err := ht.RemoveSparseBitsetChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "removed sparse bitset values", Value: strconv.Itoa(removed)}
	case "HASSB", "SBHAS", "SBEXISTS":
		values, err := sparseBitsetValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		hit, err := ht.HasSparseBitsetChecked(key, values[0])
		if err != nil {
			return commandError(err.Error())
		}
		if hit {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "COUNTSB", "SBCOUNT":
		count, ok, err := ht.CountSparseBitsetChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatUint(count, 10)}
	case "GETSB", "SBGET":
		values, ok, err := ht.GetSparseBitsetChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", values)
	case "INFOSB", "SBINFO":
		info, ok, err := ht.SparseBitsetInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATERT", "CREATERADIX", "RTCREATE":
		if err := ht.UpsertRadixTreeChecked(key); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created radix tree"}
	case "PUTRT", "RTPUT":
		fields, ok := commandRadixTreeFields(request)
		if !ok {
			return commandError("subkey/value or pairs is required")
		}
		added, err := ht.PutRadixTreeEntriesChecked(key, fields)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "stored radix tree values", Value: strconv.Itoa(added)}
	case "GETRT", "RTGET":
		subkey, ok := commandRadixTreeSubkey(request)
		if !ok {
			return commandError("subkey is required")
		}
		value, ok, err := ht.GetRadixTreeChecked(key, subkey)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "DELRT", "REMRT", "RTDEL", "RTREM":
		subkey, ok := commandRadixTreeSubkey(request)
		if !ok {
			return commandError("subkey is required")
		}
		deleted, err := ht.DeleteRadixTreeChecked(key, subkey)
		if err != nil {
			return commandError(err.Error())
		}
		if deleted {
			return CacheCommandResponse{OK: true, Message: "removed radix tree value", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "value not found", Value: "0"}
	case "HASRT", "RTEXISTS", "RTHAS":
		subkey, ok := commandRadixTreeSubkey(request)
		if !ok {
			return commandError("subkey is required")
		}
		hit, err := ht.HasRadixTreeChecked(key, subkey)
		if err != nil {
			return commandError(err.Error())
		}
		if hit {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "PREFIXRT", "SCANRT", "RTPREFIX", "RTSCAN":
		items, ok, err := ht.ScanRadixTreeChecked(key, strings.TrimSpace(request.Subkey))
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", items)
	case "INFORT", "RTINFO":
		info, ok, err := ht.RadixTreeInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATECMS", "RESERVECMS", "CMSRESERVE":
		width, depth, err := commandCountMinSketchConfig(request)
		if err != nil {
			return commandError(err.Error())
		}
		if err := ht.UpsertCountMinSketch(key, width, depth); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created count-min sketch"}
	case "INCRCMS", "ADDCMS", "CMSADD":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		count, err := commandCountMinSketchIncrement(request)
		if err != nil {
			return commandError(err.Error())
		}
		if len(values) == 1 {
			estimate, err := ht.IncrementCountMinSketchChecked(key, values[0], count)
			if err != nil {
				return commandError(err.Error())
			}
			return CacheCommandResponse{OK: true, Message: "incremented count-min sketch", Value: strconv.FormatUint(estimate, 10)}
		}
		if _, err := ht.IncrementCountMinSketchChecked(key, values[0], count, values[1:]...); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "incremented count-min sketch values", Value: strconv.Itoa(len(values))}
	case "ESTCMS", "QUERYCMS", "CMSQUERY", "CMSCOUNT":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value is required")
		}
		estimate, ok, err := ht.EstimateCountMinSketchChecked(key, values[0])
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatUint(estimate, 10)}
	case "INFOCMS", "CMSINFO":
		info, ok, err := ht.CountMinSketchInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATEHLL", "RESERVEHLL", "HLLRESERVE":
		precision, err := commandHyperLogLogPrecision(request)
		if err != nil {
			return commandError(err.Error())
		}
		if err := ht.UpsertHyperLogLog(key, precision); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created hyperloglog"}
	case "ADDHLL", "HLLADD":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		estimate, err := ht.AddHyperLogLogChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "added hyperloglog values", Value: strconv.FormatUint(estimate, 10)}
	case "COUNTHLL", "ESTHLL", "HLLCOUNT", "HLLCARD":
		count, ok, err := ht.CountHyperLogLogChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatUint(count, 10)}
	case "INFOHLL", "HLLINFO":
		info, ok, err := ht.HyperLogLogInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATETOPK", "RESERVETOPK", "TOPKRESERVE":
		capacity, err := commandTopKCapacity(request)
		if err != nil {
			return commandError(err.Error())
		}
		if err := ht.UpsertTopK(key, capacity); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created top-k"}
	case "ADDTOPK", "TOPKADD":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		count, err := commandTopKCount(request)
		if err != nil {
			return commandError(err.Error())
		}
		if len(values) == 1 {
			estimate, err := ht.AddTopKChecked(key, values[0], count)
			if err != nil {
				return commandError(err.Error())
			}
			return commandValueResponse("added top-k value", estimate)
		}
		if _, err := ht.AddTopKChecked(key, values[0], count, values[1:]...); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "added top-k values", Value: strconv.Itoa(len(values))}
	case "ESTTOPK", "QUERYTOPK", "TOPKCOUNT":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value is required")
		}
		estimate, err := ht.EstimateTopKChecked(key, values[0])
		if err != nil {
			return commandError(err.Error())
		}
		return commandValueResponse("ok", estimate)
	case "GETTOPK", "TOPK":
		value, ok, err := ht.GetTopKChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "INFOTOPK", "TOPKINFO":
		info, ok, err := ht.TopKInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATERS", "CREATESAMPLE", "RESERVERS", "RSRESERVE":
		capacity, err := commandReservoirSampleCapacity(request)
		if err != nil {
			return commandError(err.Error())
		}
		if err := ht.UpsertReservoirSample(key, capacity); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created reservoir sample"}
	case "ADDRS", "RSADD":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		update, err := ht.AddReservoirSampleChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return commandValueResponse("added reservoir sample values", update)
	case "GETRS", "RSGET", "SAMPLE":
		value, ok, err := ht.GetReservoirSampleChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "INFORS", "RSINFO":
		info, ok, err := ht.ReservoirSampleInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATEQ", "CREATEQS", "CREATEQUANTILE", "RESERVEQ", "QSRESERVE":
		epsilon, err := commandQuantileSketchEpsilon(request)
		if err != nil {
			return commandError(err.Error())
		}
		if err := ht.UpsertQuantileSketch(key, epsilon); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created quantile sketch"}
	case "ADDQ", "ADDQS", "QADD", "QSADD":
		values, err := quantileSketchValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		estimate, err := ht.AddQuantileSketchChecked(key, values[0], values[1:]...)
		if err != nil {
			return commandError(err.Error())
		}
		return commandValueResponse("added quantile sketch values", estimate)
	case "ESTQ", "QUERYQ", "QQUERY", "QSQUERY", "QUANTILE":
		quantile, err := commandQuantileValue(request)
		if err != nil {
			return commandError(err.Error())
		}
		estimate, ok, err := ht.EstimateQuantileSketchChecked(key, quantile)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", estimate)
	case "INFOQ", "QINFO", "INFOQS", "QSINFO":
		info, ok, err := ht.QuantileSketchInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATEFW", "CREATEFENWICK", "RESERVEFW", "FWRESERVE":
		size, err := commandFenwickTreeSize(request)
		if err != nil {
			return commandError(err.Error())
		}
		if err := ht.UpsertFenwickTree(key, size); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "created fenwick tree"}
	case "ADDFW", "FWADD":
		index, delta, err := commandFenwickTreeUpdate(request)
		if err != nil {
			return commandError(err.Error())
		}
		update, ok, err := ht.AddFenwickTreeChecked(key, index, delta)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return commandError("fenwick tree update is out of range or overflows")
		}
		return commandValueResponse("updated fenwick tree", update)
	case "GETFW", "FWGET":
		index, err := commandFenwickTreeIndex(request)
		if err != nil {
			return commandError(err.Error())
		}
		value, ok, err := ht.GetFenwickTreeChecked(key, index)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatInt(value, 10)}
	case "SUMFW", "PREFIXFW", "FWPREFIX", "FWSUM":
		index, err := commandFenwickTreeIndex(request)
		if err != nil {
			return commandError(err.Error())
		}
		value, ok, err := ht.PrefixSumFenwickTreeChecked(key, index)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatInt(value, 10)}
	case "RANGEFW", "FWRANGE":
		start, end, err := commandFenwickTreeRange(request)
		if err != nil {
			return commandError(err.Error())
		}
		value, ok, err := ht.RangeSumFenwickTreeChecked(key, start, end)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatInt(value, 10)}
	case "INFOFW", "FWINFO":
		info, ok, err := ht.FenwickTreeInfoChecked(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	default:
		return commandError("unsupported command")
	}
}

func (ht *HatTrie) executePublicBatchCommand(request CacheCommandRequest) CacheCommandResponse {
	if response, ok := ht.executePublicScalarBatchCommand(request); ok {
		return response
	}
	return executePublicCommandBatchRequests(request, ht.ExecuteCommand)
}

func executePublicCommandBatchRequests(request CacheCommandRequest, execute func(CacheCommandRequest) CacheCommandResponse) CacheCommandResponse {
	payloads, err := publicCommandBatchRequests(request)
	if err != nil {
		return commandError(err.Error())
	}
	responses := make([]CacheCommandResponse, 0, len(payloads))
	allOK := true
	for idx, payload := range payloads {
		if err := validatePublicCommandBatchPayload(payload, idx); err != nil {
			responses = append(responses, commandError(err.Error()))
			allOK = false
			continue
		}
		response := execute(payload)
		if !response.OK {
			allOK = false
		}
		responses = append(responses, response)
	}
	return publicCommandBatchResponse(responses, allOK)
}

func (ht *HatTrie) executePublicScalarBatchCommand(request CacheCommandRequest) (CacheCommandResponse, bool) {
	if response, ok := ht.executePublicNativeScalarBatchCommand(request); ok {
		return response, true
	}
	return ht.executePublicScalarBatchCommandWithExecutor(request, nil)
}

type publicScalarBatchPayloadExecutor func(index int, request CacheCommandRequest, execute func() CacheCommandResponse) (CacheCommandResponse, bool)

func (ht *HatTrie) executePublicScalarBatchCommandWithExecutor(request CacheCommandRequest, executor publicScalarBatchPayloadExecutor) (CacheCommandResponse, bool) {
	payloads, err := publicCommandBatchRequests(request)
	if err != nil {
		return commandError(err.Error()), true
	}
	for _, payload := range payloads {
		if _, _, supported := publicScalarBatchCommandCode(payload.Command); !supported {
			return CacheCommandResponse{}, false
		}
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	if executor == nil {
		responses := make([]CacheCommandResponse, len(payloads))
		allOK := true
		for idx, payload := range payloads {
			response := ht.executePublicScalarBatchPayloadLocked(payload, idx)
			if !response.OK {
				allOK = false
			}
			responses[idx] = response
		}
		return publicCommandBatchResponse(responses, allOK), true
	}

	responses := make([]CacheCommandResponse, len(payloads))
	responseCount := len(payloads)
	allOK := true
	for idx, payload := range payloads {
		response, stop := executor(idx, payload, func() CacheCommandResponse {
			return ht.executePublicScalarBatchPayloadLocked(payload, idx)
		})
		if !response.OK {
			allOK = false
		}
		responses[idx] = response
		if stop {
			responseCount = idx + 1
			break
		}
	}
	return publicCommandBatchResponse(responses[:responseCount], allOK), true
}

type publicScalarBatchCommand uint8

const (
	publicScalarBatchInvalid publicScalarBatchCommand = iota
	publicScalarBatchGet
	publicScalarBatchExists
	publicScalarBatchSetString
	publicScalarBatchSetStringTTL
	publicScalarBatchSetCounter
	publicScalarBatchSetCounterTTL
	publicScalarBatchIncrement
	publicScalarBatchDelete
	publicScalarBatchTTL
	publicScalarBatchExpire
	publicScalarBatchExpireAt
	publicScalarBatchPersist
)

func publicScalarBatchCommandCode(command string) (publicScalarBatchCommand, string, bool) {
	switch command {
	case "":
		return publicScalarBatchInvalid, "command is required", true
	case "BATCH":
		return publicScalarBatchInvalid, "nested BATCH is not supported", true
	case "INTERNALSET", "INTERNALDEL", "INTERNALBATCH", replicationBatchEnvelopeCommand, replicationSetBinaryCommand, replicationSetCompactCommand, replicationDigestCommand:
		return publicScalarBatchInvalid, "internal replication command " + command + " is not allowed", true
	case "GET", "GETSTR":
		return publicScalarBatchGet, "", true
	case "EXISTS":
		return publicScalarBatchExists, "", true
	case "SET", "SETSTR":
		return publicScalarBatchSetString, "", true
	case "SETX", "SETSTRX":
		return publicScalarBatchSetStringTTL, "", true
	case "SETINT":
		return publicScalarBatchSetCounter, "", true
	case "SETINTX":
		return publicScalarBatchSetCounterTTL, "", true
	case "INC":
		return publicScalarBatchIncrement, "", true
	case "DEL":
		return publicScalarBatchDelete, "", true
	case "TTL":
		return publicScalarBatchTTL, "", true
	case "EXPIRE":
		return publicScalarBatchExpire, "", true
	case "EXPIREAT":
		return publicScalarBatchExpireAt, "", true
	case "PERSIST":
		return publicScalarBatchPersist, "", true
	default:
		return publicScalarBatchInvalid, "", false
	}
}

func (ht *HatTrie) executePublicScalarBatchPayloadLocked(request CacheCommandRequest, index int) CacheCommandResponse {
	command, validationMessage, supported := publicScalarBatchCommandCode(request.Command)
	if !supported {
		return commandError("unsupported command")
	}
	if validationMessage != "" {
		return commandError(fmt.Sprintf("batch value %d: %s", index, validationMessage))
	}
	key := strings.TrimSpace(request.Key)
	if key == "" {
		return commandError("key is required")
	}
	if err := validateKey(key); err != nil {
		return commandError(err.Error())
	}

	switch command {
	case publicScalarBatchGet:
		value, ok, err := ht.commandValueLockedForKey(key)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return CacheCommandResponse{OK: true, Message: "key not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: value}
	case publicScalarBatchExists:
		hval := ht.peekLocked(key)
		hit := !hval.Empty()
		ht.recordReadLocked(hit, key)
		if hit {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case publicScalarBatchSetString:
		if response, ok := validateOptionalCommandExpiration(request.TTLSeconds, request.UnixSeconds); ok && !response.OK {
			return response
		}
		if err := ht.upsertStringLocked(key, request.Value); err != nil {
			return commandError(err.Error())
		}
		if response, ok := ht.applyCommandExpirationLocked(key, request.TTLSeconds, request.UnixSeconds); ok {
			return response
		}
		return CacheCommandResponse{OK: true, Message: "stored string"}
	case publicScalarBatchSetStringTTL:
		ttl, ok := requirePositiveTTL(request.TTLSeconds)
		if !ok {
			return commandError("positive ttl_seconds is required")
		}
		if err := ht.upsertStringLocked(key, request.Value); err != nil {
			return commandError(err.Error())
		}
		if !ht.expireRelativeLocked(key, ttl) {
			return commandError("failed to set ttl")
		}
		return CacheCommandResponse{OK: true, Message: "stored string with ttl"}
	case publicScalarBatchSetCounter:
		value, ok := parseCommandInt32(request.Value)
		if !ok {
			return commandError("value must be a 32-bit integer")
		}
		if response, ok := validateOptionalCommandExpiration(request.TTLSeconds, request.UnixSeconds); ok && !response.OK {
			return response
		}
		if err := ht.upsertCounterLocked(key, value); err != nil {
			return commandError(err.Error())
		}
		if response, ok := ht.applyCommandExpirationLocked(key, request.TTLSeconds, request.UnixSeconds); ok {
			return response
		}
		return CacheCommandResponse{OK: true, Message: "stored counter"}
	case publicScalarBatchSetCounterTTL:
		value, ok := parseCommandInt32(request.Value)
		if !ok {
			return commandError("value must be a 32-bit integer")
		}
		ttl, ok := requirePositiveTTL(request.TTLSeconds)
		if !ok {
			return commandError("positive ttl_seconds is required")
		}
		if err := ht.upsertCounterLocked(key, value); err != nil {
			return commandError(err.Error())
		}
		if !ht.expireRelativeLocked(key, ttl) {
			return commandError("failed to set ttl")
		}
		return CacheCommandResponse{OK: true, Message: "stored counter with ttl"}
	case publicScalarBatchIncrement:
		by, ok := parseCommandIncrement(request.Value)
		if !ok {
			return commandError("value must be a 32-bit integer")
		}
		value, ok, err := ht.incrementCounterLocked(key, by, true)
		if err != nil {
			return commandError(err.Error())
		}
		if !ok {
			return commandError("counter overflow")
		}
		return CacheCommandResponse{OK: true, Message: "incremented", Value: strconv.FormatInt(int64(value), 10)}
	case publicScalarBatchDelete:
		if ht.deleteAndRecordLocked(key) {
			return CacheCommandResponse{OK: true, Message: "deleted"}
		}
		return CacheCommandResponse{OK: true, Message: "key not found"}
	case publicScalarBatchTTL:
		ttl := ht.ttlLocked(key)
		if ttl == NoTTL {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "-1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatInt(int64(ttl/time.Second), 10)}
	case publicScalarBatchExpire:
		ttl, ok := requirePositiveTTL(request.TTLSeconds)
		if !ok {
			return commandError("positive ttl_seconds is required")
		}
		if !ht.expireRelativeLocked(key, ttl) {
			return CacheCommandResponse{OK: true, Message: "key not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ttl updated"}
	case publicScalarBatchExpireAt:
		if request.UnixSeconds == nil {
			return commandError("unix_seconds is required")
		}
		if !ht.expireAtAndRecordLocked(key, time.Unix(*request.UnixSeconds, 0)) {
			return CacheCommandResponse{OK: true, Message: "key not found"}
		}
		return CacheCommandResponse{OK: true, Message: "expiration updated"}
	case publicScalarBatchPersist:
		if !ht.persistLocked(key) {
			return CacheCommandResponse{OK: true, Message: "key not found or no ttl"}
		}
		return CacheCommandResponse{OK: true, Message: "ttl removed"}
	default:
		return commandError("unsupported command")
	}
}

func publicCommandBatchRequests(request CacheCommandRequest) ([]CacheCommandRequest, error) {
	if len(request.Batch) == 0 {
		return nil, errors.New("batch requires requests")
	}
	if len(request.Batch) > maxPublicCommandBatchSize {
		return nil, fmt.Errorf("batch size must be <= %d", maxPublicCommandBatchSize)
	}
	return request.Batch, nil
}

func validatePublicCommandBatchPayload(request CacheCommandRequest, index int) error {
	command := normalizedCommand(request.Command)
	if command == "" {
		return fmt.Errorf("batch value %d: command is required", index)
	}
	switch command {
	case "BATCH":
		return fmt.Errorf("batch value %d: nested BATCH is not supported", index)
	case "INTERNALSET", "INTERNALDEL", "INTERNALBATCH", replicationBatchEnvelopeCommand, replicationSetBinaryCommand, replicationSetCompactCommand, replicationDigestCommand:
		return fmt.Errorf("batch value %d: internal replication command %s is not allowed", index, command)
	default:
		return nil
	}
}

func publicCommandBatchResponse(responses []CacheCommandResponse, ok bool) CacheCommandResponse {
	message := "batch applied"
	if !ok {
		message = "batch completed with errors"
	}
	return CacheCommandResponse{OK: ok, Message: message, Responses: responses}
}

func (ht *HatTrie) executeExactFastCommand(request CacheCommandRequest) (CacheCommandResponse, bool) {
	key := request.Key
	if !commandFastPathField(key) || !validKey(key) {
		return CacheCommandResponse{}, false
	}
	switch request.Command {
	case "GET", "GETSTR":
		return ht.executeFastGetCommand(key)
	case "DUMP":
		return ht.executeFastDumpCommand(key), true
	case "PUTMAP":
		if len(request.Pairs) != 0 || !commandFastPathField(request.Subkey) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastPutMapCommand(key, request.Subkey, request.Value)
	case "PUTRT", "RTPUT":
		if len(request.Pairs) != 0 || !commandFastPathField(request.Subkey) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastPutRadixTreeCommand(key, request.Subkey, request.Value)
	case "PREFIXRT", "SCANRT", "RTPREFIX", "RTSCAN":
		prefix := strings.TrimSpace(request.Subkey)
		if prefix != request.Subkey {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastScanRadixTreeCommand(key, prefix)
	case "PUSHSLICE":
		if len(request.Values) != 0 || request.Value == "" {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastPushSliceCommand(key, request.Value)
	case "POPSLICE":
		return ht.executeFastPopSliceCommand(key)
	case "ADDSET":
		if len(request.Values) != 0 {
			return CacheCommandResponse{}, false
		}
		if !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastAddSetCommand(key, request.Value)
	case "HASSET":
		if len(request.Values) != 0 {
			return CacheCommandResponse{}, false
		}
		if !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastHasSetCommand(key, request.Value)
	case "PUSHPQ", "PUSHPRIORITY":
		if len(request.Values) != 0 || !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		priority, ok := commandPriority(request)
		if !ok {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastPushPriorityQueueCommand(key, priority, request.Value)
	case "POPPQ", "POPPRIORITY":
		return ht.executeFastPopPriorityQueueCommand(key)
	case "ADDBF", "BFADD":
		if len(request.Values) != 0 || !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastAddBloomFilterCommand(key, request.Value)
	case "HASBF", "BFHAS", "BFEXISTS":
		if len(request.Values) != 0 || !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastHasBloomFilterCommand(key, request.Value)
	case "ADDCF", "CFADD":
		if len(request.Values) != 0 || !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastAddCuckooFilterCommand(key, request.Value)
	case "HASCF", "CFHAS", "CFEXISTS":
		if len(request.Values) != 0 || !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastHasCuckooFilterCommand(key, request.Value)
	case "DELCF", "REMCF", "CFDEL":
		if len(request.Values) != 0 || !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastDeleteCuckooFilterCommand(key, request.Value)
	case "INCRCMS", "ADDCMS", "CMSADD":
		if len(request.Values) != 0 || !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		count, err := commandCountMinSketchIncrement(request)
		if err != nil {
			return commandError(err.Error()), true
		}
		return ht.executeFastIncrementCountMinSketchCommand(key, request.Value, count)
	case "ESTCMS", "QUERYCMS", "CMSQUERY", "CMSCOUNT":
		if len(request.Values) != 0 || !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastEstimateCountMinSketchCommand(key, request.Value)
	case "PEEKMAP":
		if !commandFastPathField(request.Subkey) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastPeekMapCommand(key, request.Subkey)
	case "HASRB":
		value, ok := commandFastUint32Field(request.Value)
		if !ok {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastHasRoaringBitmapCommand(key, value)
	case "ADDRB":
		if len(request.Values) != 0 {
			return CacheCommandResponse{}, false
		}
		value, ok := commandFastUint32Field(request.Value)
		if !ok {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastAddRoaringBitmapCommand(key, value)
	case "HASSB":
		value, ok := commandFastUint64Field(request.Value)
		if !ok {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastHasSparseBitsetCommand(key, value)
	case "ADDSB":
		if len(request.Values) != 0 {
			return CacheCommandResponse{}, false
		}
		value, ok := commandFastUint64Field(request.Value)
		if !ok {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastAddSparseBitsetCommand(key, value)
	case "ADDHLL", "HLLADD":
		if len(request.Values) != 0 || !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastAddHyperLogLogCommand(key, request.Value)
	case "COUNTHLL", "ESTHLL", "HLLCOUNT", "HLLCARD":
		return ht.executeFastCountHyperLogLogCommand(key)
	case "ADDTOPK", "TOPKADD":
		if len(request.Values) != 0 || len(request.Pairs) != 0 || !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		count, ok := commandFastOptionalUint64Field(request.Subkey, 1)
		if !ok || count == 0 {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastAddTopKCommand(key, request.Value, count)
	case "GETTOPK", "TOPK":
		return ht.executeFastGetTopKCommand(key)
	case "ADDRS", "RSADD":
		if len(request.Values) != 0 || len(request.Pairs) != 0 || !commandFastJSONPlainString(request.Value) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastAddReservoirSampleCommand(key, request.Value)
	case "ADDQ", "ADDQS", "QADD", "QSADD":
		if len(request.Values) != 0 || len(request.Pairs) != 0 {
			return CacheCommandResponse{}, false
		}
		value, ok := commandFastFloat64Field(request.Value)
		if !ok || !validQuantileSketchValue(value) {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastAddQuantileSketchCommand(key, value)
	case "ESTQ", "QUERYQ", "QQUERY", "QSQUERY", "QUANTILE":
		if len(request.Values) != 0 || len(request.Pairs) != 0 {
			return CacheCommandResponse{}, false
		}
		raw := request.Value
		if request.Subkey != "" {
			raw = request.Subkey
		}
		quantile, ok := commandFastFloat64Field(raw)
		if !ok || quantile < 0 || quantile > 1 {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastEstimateQuantileSketchCommand(key, quantile)
	case "ADDFW", "FWADD":
		if len(request.Values) != 0 || len(request.Pairs) != 0 {
			return CacheCommandResponse{}, false
		}
		index, ok := commandFastUint64Field(request.Value)
		if !ok {
			return CacheCommandResponse{}, false
		}
		delta, ok := commandFastInt64Field(request.Subkey)
		if !ok || delta == 0 {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastAddFenwickTreeCommand(key, index, delta)
	case "RANGEFW", "FWRANGE":
		if len(request.Values) != 0 || len(request.Pairs) != 0 {
			return CacheCommandResponse{}, false
		}
		start, ok := commandFastUint64Field(request.Value)
		if !ok {
			return CacheCommandResponse{}, false
		}
		end, ok := commandFastUint64Field(request.Subkey)
		if !ok || start > end {
			return CacheCommandResponse{}, false
		}
		return ht.executeFastRangeFenwickTreeCommand(key, start, end)
	default:
		return CacheCommandResponse{}, false
	}
}

func (ht *HatTrie) executeFastDumpCommand(key string) CacheCommandResponse {
	value, ok, err := ht.commandDumpEntry(key)
	if err != nil {
		return commandError(err.Error())
	}
	if !ok {
		return CacheCommandResponse{OK: true, Message: "key not found"}
	}
	return CacheCommandResponse{OK: true, Message: "ok", Value: value}
}

func (ht *HatTrie) executeFastPutMapCommand(key string, subkey string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
		} else if hval.IsMap() {
			ht.maps.PutEntry(hval.Index, subkey, value)
			ht.recordWriteLocked(key)
			ht.cacheValueLocked(key, hval)
			return CacheCommandResponse{OK: true, Message: "stored map fields"}, true
		}
	}

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if hval.IsMap() {
		ht.maps.PutEntry(hval.Index, subkey, value)
		if rawPtr != nil {
			*rawPtr = hval.toValue()
		}
		ht.recordWriteLocked(key)
		ht.cacheValueLocked(key, hval)
		return CacheCommandResponse{OK: true, Message: "stored map fields"}, true
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.maps.AddEntry(subkey, value)
	hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_MAP}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "stored map fields"}, true
}

func (ht *HatTrie) executeFastPutRadixTreeCommand(key string, subkey string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
		} else if hval.IsRadixTree() {
			added := ht.radixTrees.array[hval.Index].Put(subkey, value)
			ht.recordWriteLocked(key)
			ht.cacheValueLocked(key, hval)
			return CacheCommandResponse{OK: true, Message: "stored radix tree values", Value: strconv.Itoa(boolInt(added))}, true
		}
	}

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if hval.IsRadixTree() {
		added := ht.radixTrees.array[hval.Index].Put(subkey, value)
		if rawPtr != nil {
			*rawPtr = hval.toValue()
		}
		ht.recordWriteLocked(key)
		ht.cacheValueLocked(key, hval)
		return CacheCommandResponse{OK: true, Message: "stored radix tree values", Value: strconv.Itoa(boolInt(added))}, true
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	data := newRadixTreeData()
	added := data.Put(subkey, value)
	idx := ht.radixTrees.AddData(data)
	hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RADIX_TREE}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "stored radix tree values", Value: strconv.Itoa(boolInt(added))}, true
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (ht *HatTrie) executeFastPushSliceCommand(key string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
		} else if hval.IsSlice() {
			if err := ht.slices.array[hval.Index].PushOneChecked(value); err != nil {
				return commandError(err.Error()), true
			}
			ht.recordWriteLocked(key)
			ht.cacheValueLocked(key, hval)
			return CacheCommandResponse{OK: true, Message: "pushed slice values"}, true
		}
	}

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if hval.IsSlice() {
		if err := ht.slices.array[hval.Index].PushOneChecked(value); err != nil {
			return commandError(err.Error()), true
		}
		if rawPtr != nil {
			*rawPtr = hval.toValue()
		}
		ht.recordWriteLocked(key)
		ht.cacheValueLocked(key, hval)
		return CacheCommandResponse{OK: true, Message: "pushed slice values"}, true
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx, err := ht.slices.AddValuesChecked(value)
	if err != nil {
		return commandError(err.Error()), true
	}
	hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SLICE}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "pushed slice values"}, true
}

func (ht *HatTrie) executeFastPopSliceCommand(key string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsSlice() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	value, ok := ht.slices.array[hval.Index].popRetain()
	if !ok {
		ht.recordReadLocked(false, key)
		ht.cacheValueLocked(key, hval)
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	ht.recordReadLocked(true, key)
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	if text, ok := value.(string); ok {
		return CacheCommandResponse{OK: true, Message: "removed", Value: text}, true
	}
	return commandValueResponse("removed", value), true
}

func (ht *HatTrie) executeFastAddSetCommand(key string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
		} else if hval.IsSet() {
			data := &ht.sets.array[hval.Index]
			added := data.addPlainString(value)
			if added > 0 {
				ht.recordWriteLocked(key)
				ht.cacheValueLocked(key, hval)
			}
			return CacheCommandResponse{OK: true, Message: "added set values", Value: strconv.Itoa(added)}, true
		}
	}

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if hval.IsSet() {
		data := &ht.sets.array[hval.Index]
		added := data.addPlainString(value)
		if rawPtr != nil {
			*rawPtr = hval.toValue()
		}
		if added > 0 {
			ht.recordWriteLocked(key)
		}
		ht.cacheValueLocked(key, hval)
		return CacheCommandResponse{OK: true, Message: "added set values", Value: strconv.Itoa(added)}, true
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	data := setData{}
	added := data.addPlainString(value)
	idx := ht.sets.AddData(data)
	hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SET}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "added set values", Value: strconv.Itoa(added)}, true
}

func (ht *HatTrie) executeFastHasSetCommand(key string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsSet() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}, true
	}
	hit := ht.sets.array[hval.Index].hasPlainString(value)
	ht.recordReadLocked(hit, key)
	return commandBool01Response(hit), true
}

func (ht *HatTrie) executeFastScanRadixTreeCommand(key string, prefix string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if !hval.IsRadixTree() {
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	payload, ok := ht.radixTrees.array[hval.Index].plainItemsWithPrefixJSON(prefix)
	if !ok {
		return CacheCommandResponse{}, false
	}
	ht.recordReadLocked(true, key)
	return CacheCommandResponse{OK: true, Message: "ok", Value: payload}, true
}

func (ht *HatTrie) executeFastPushPriorityQueueCommand(key string, priority int64, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
		} else if hval.IsPriorityQueue() {
			if err := ht.priorityQueues.array[hval.Index].PushStringChecked(priority, value); err != nil {
				return commandError(err.Error()), true
			}
			ht.recordWriteLocked(key)
			ht.cacheValueLocked(key, hval)
			return CacheCommandResponse{OK: true, Message: "pushed priority queue values", Value: "1"}, true
		}
	}

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if hval.IsPriorityQueue() {
		if err := ht.priorityQueues.array[hval.Index].PushStringChecked(priority, value); err != nil {
			return commandError(err.Error()), true
		}
		if rawPtr != nil {
			*rawPtr = hval.toValue()
		}
		ht.recordWriteLocked(key)
		ht.cacheValueLocked(key, hval)
		return CacheCommandResponse{OK: true, Message: "pushed priority queue values", Value: "1"}, true
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.priorityQueues.Add(nil)
	if err := ht.priorityQueues.array[idx].PushStringChecked(priority, value); err != nil {
		ht.priorityQueues.Del(idx)
		return commandError(err.Error()), true
	}
	hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_PRIORITY_QUEUE}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "pushed priority queue values", Value: "1"}, true
}

func (ht *HatTrie) executeFastPopPriorityQueueCommand(key string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsPriorityQueue() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	item, ok := ht.priorityQueues.array[hval.Index].popItemRetain()
	if !ok {
		ht.recordReadLocked(false, key)
		ht.cacheValueLocked(key, hval)
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	ht.recordReadLocked(true, key)
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	if text, ok := item.value().(string); ok {
		if payload, ok := commandFastPriorityQueueItemJSON(item.Priority, text); ok {
			return CacheCommandResponse{OK: true, Message: "removed", Value: payload}, true
		}
	}
	return commandValueResponse("removed", item.PriorityItem()), true
}

func (ht *HatTrie) executeFastAddBloomFilterCommand(key string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
		} else if hval.IsBloomFilter() {
			added := boolInt(ht.bloomFilters.array[hval.Index].addJSONString(value))
			if added > 0 {
				ht.recordWriteLocked(key)
				ht.cacheValueLocked(key, hval)
			}
			return CacheCommandResponse{OK: true, Message: "added bloom filter values", Value: strconv.Itoa(added)}, true
		}
	}

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if hval.IsBloomFilter() {
		added := boolInt(ht.bloomFilters.array[hval.Index].addJSONString(value))
		if rawPtr != nil {
			*rawPtr = hval.toValue()
		}
		if added > 0 {
			ht.recordWriteLocked(key)
		}
		ht.cacheValueLocked(key, hval)
		return CacheCommandResponse{OK: true, Message: "added bloom filter values", Value: strconv.Itoa(added)}, true
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	data := newDefaultBloomFilterData()
	added := boolInt(data.addJSONString(value))
	idx := ht.bloomFilters.AddData(data)
	hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_BLOOM_FILTER}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "added bloom filter values", Value: strconv.Itoa(added)}, true
}

func (ht *HatTrie) executeFastHasBloomFilterCommand(key string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsBloomFilter() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}, true
	}
	hit := ht.bloomFilters.array[hval.Index].containsJSONString(value)
	ht.recordReadLocked(hit, key)
	return commandBool01Response(hit), true
}

func (ht *HatTrie) executeFastAddCuckooFilterCommand(key string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
		} else if hval.IsCuckooFilter() {
			added := boolInt(ht.cuckooFilters.array[hval.Index].addJSONString(value))
			if added > 0 {
				ht.recordWriteLocked(key)
				ht.cacheValueLocked(key, hval)
			}
			return CacheCommandResponse{OK: true, Message: "added cuckoo filter values", Value: strconv.Itoa(added)}, true
		}
	}

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if hval.IsCuckooFilter() {
		added := boolInt(ht.cuckooFilters.array[hval.Index].addJSONString(value))
		if rawPtr != nil {
			*rawPtr = hval.toValue()
		}
		if added > 0 {
			ht.recordWriteLocked(key)
		}
		ht.cacheValueLocked(key, hval)
		return CacheCommandResponse{OK: true, Message: "added cuckoo filter values", Value: strconv.Itoa(added)}, true
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	data := newDefaultCuckooFilterData()
	added := boolInt(data.addJSONString(value))
	idx := ht.cuckooFilters.AddData(data)
	hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_CUCKOO_FILTER}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "added cuckoo filter values", Value: strconv.Itoa(added)}, true
}

func (ht *HatTrie) executeFastHasCuckooFilterCommand(key string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsCuckooFilter() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}, true
	}
	hit := ht.cuckooFilters.array[hval.Index].containsJSONString(value)
	ht.recordReadLocked(hit, key)
	return commandBool01Response(hit), true
}

func (ht *HatTrie) executeFastDeleteCuckooFilterCommand(key string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsCuckooFilter() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "removed cuckoo filter values", Value: "0"}, true
	}
	deleted := boolInt(ht.cuckooFilters.array[hval.Index].deleteJSONString(value))
	ht.recordReadLocked(deleted > 0, key)
	if deleted > 0 {
		ht.recordWriteLocked(key)
		ht.cacheValueLocked(key, hval)
	}
	return CacheCommandResponse{OK: true, Message: "removed cuckoo filter values", Value: strconv.Itoa(deleted)}, true
}

func (ht *HatTrie) executeFastIncrementCountMinSketchCommand(key string, value string, count uint32) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
		} else if hval.IsCountMinSketch() {
			estimate := ht.countMinSketches.array[hval.Index].addJSONString(value, count)
			ht.recordWriteLocked(key)
			ht.cacheValueLocked(key, hval)
			return CacheCommandResponse{OK: true, Message: "incremented count-min sketch", Value: strconv.FormatUint(estimate, 10)}, true
		}
	}

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if hval.IsCountMinSketch() {
		estimate := ht.countMinSketches.array[hval.Index].addJSONString(value, count)
		if rawPtr != nil {
			*rawPtr = hval.toValue()
		}
		ht.recordWriteLocked(key)
		ht.cacheValueLocked(key, hval)
		return CacheCommandResponse{OK: true, Message: "incremented count-min sketch", Value: strconv.FormatUint(estimate, 10)}, true
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	data := newDefaultCountMinSketchData()
	estimate := data.addJSONString(value, count)
	idx := ht.countMinSketches.AddData(data)
	hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_COUNT_MIN_SKETCH}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "incremented count-min sketch", Value: strconv.FormatUint(estimate, 10)}, true
}

func (ht *HatTrie) executeFastEstimateCountMinSketchCommand(key string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsCountMinSketch() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	estimate := ht.countMinSketches.array[hval.Index].estimateJSONString(value)
	ht.recordReadLocked(true, key)
	return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatUint(estimate, 10)}, true
}

func (ht *HatTrie) executeFastAddRoaringBitmapCommand(key string, value uint32) (CacheCommandResponse, bool) {
	added, err := ht.AddRoaringBitmapChecked(key, value)
	if err != nil {
		return commandError(err.Error()), true
	}
	return CacheCommandResponse{OK: true, Message: "added roaring bitmap values", Value: strconv.Itoa(added)}, true
}

func (ht *HatTrie) executeFastAddSparseBitsetCommand(key string, value uint64) (CacheCommandResponse, bool) {
	added, err := ht.AddSparseBitsetChecked(key, value)
	if err != nil {
		return commandError(err.Error()), true
	}
	return CacheCommandResponse{OK: true, Message: "added sparse bitset values", Value: strconv.Itoa(added)}, true
}

func (ht *HatTrie) executeFastAddHyperLogLogCommand(key string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
		} else if hval.IsHyperLogLog() {
			data := &ht.hyperLogLogs.array[hval.Index]
			data.addJSONString(value)
			ht.recordWriteLocked(key)
			ht.cacheValueLocked(key, hval)
			return CacheCommandResponse{OK: true, Message: "added hyperloglog values", Value: strconv.FormatUint(data.Count(), 10)}, true
		}
	}

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if hval.IsHyperLogLog() {
		data := &ht.hyperLogLogs.array[hval.Index]
		data.addJSONString(value)
		if rawPtr != nil {
			*rawPtr = hval.toValue()
		}
		ht.recordWriteLocked(key)
		ht.cacheValueLocked(key, hval)
		return CacheCommandResponse{OK: true, Message: "added hyperloglog values", Value: strconv.FormatUint(data.Count(), 10)}, true
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	data := newDefaultHyperLogLogData()
	data.addJSONString(value)
	idx := ht.hyperLogLogs.AddData(data)
	hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_HYPERLOGLOG}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "added hyperloglog values", Value: strconv.FormatUint(data.Count(), 10)}, true
}

func (ht *HatTrie) executeFastCountHyperLogLogCommand(key string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsHyperLogLog() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	count := ht.hyperLogLogs.array[hval.Index].Count()
	ht.recordReadLocked(true, key)
	return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatUint(count, 10)}, true
}

func (ht *HatTrie) executeFastAddTopKCommand(key string, value string, count uint64) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
		} else if hval.IsTopK() {
			estimate := ht.topKs.array[hval.Index].addPlainJSONString(value, count)
			ht.recordWriteLocked(key)
			ht.cacheValueLocked(key, hval)
			return commandFastTopKEstimateResponse("added top-k value", estimate), true
		}
	}

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if hval.IsTopK() {
		estimate := ht.topKs.array[hval.Index].addPlainJSONString(value, count)
		if rawPtr != nil {
			*rawPtr = hval.toValue()
		}
		ht.recordWriteLocked(key)
		ht.cacheValueLocked(key, hval)
		return commandFastTopKEstimateResponse("added top-k value", estimate), true
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	data := newDefaultTopKData()
	estimate := data.addPlainJSONString(value, count)
	idx := ht.topKs.AddData(data)
	hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_TOP_K}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	return commandFastTopKEstimateResponse("added top-k value", estimate), true
}

func (ht *HatTrie) executeFastGetTopKCommand(key string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsTopK() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	top := ht.topKs.array[hval.Index]
	ht.recordReadLocked(true, key)
	if payload, ok := commandFastTopKItemsJSON(top); ok {
		return CacheCommandResponse{OK: true, Message: "ok", Value: payload}, true
	}
	return commandValueResponse("ok", top.Items()), true
}

func (ht *HatTrie) executeFastAddReservoirSampleCommand(key string, value string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	if hval, ok := ht.cachedValueLocked(key); ok {
		if ht.expireIfNeededLocked(key, hval) {
			ht.clearHotKeyLocked(key)
		} else if hval.IsReservoirSample() {
			update, err := ht.reservoirSamples.array[hval.Index].AddPlainJSONStringChecked(value)
			if err != nil {
				return commandError(err.Error()), true
			}
			ht.recordWriteLocked(key)
			ht.cacheValueLocked(key, hval)
			return commandFastReservoirSampleUpdateResponse("added reservoir sample values", update), true
		}
	}

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error()), true
	}
	if hval.IsReservoirSample() {
		update, err := ht.reservoirSamples.array[hval.Index].AddPlainJSONStringChecked(value)
		if err != nil {
			return commandError(err.Error()), true
		}
		if rawPtr != nil {
			*rawPtr = hval.toValue()
		}
		ht.recordWriteLocked(key)
		ht.cacheValueLocked(key, hval)
		return commandFastReservoirSampleUpdateResponse("added reservoir sample values", update), true
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	data := newDefaultReservoirSampleData()
	update, err := data.AddPlainJSONStringChecked(value)
	if err != nil {
		return commandError(err.Error()), true
	}
	idx := ht.reservoirSamples.AddData(data)
	hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RESERVOIR_SAMPLE}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	ht.cacheValueLocked(key, hval)
	return commandFastReservoirSampleUpdateResponse("added reservoir sample values", update), true
}

func (ht *HatTrie) executeFastAddQuantileSketchCommand(key string, value float64) (CacheCommandResponse, bool) {
	estimate, err := ht.AddQuantileSketchChecked(key, value)
	if err != nil {
		return commandError(err.Error()), true
	}
	return commandFastQuantileEstimateResponse("added quantile sketch values", estimate), true
}

func (ht *HatTrie) executeFastEstimateQuantileSketchCommand(key string, quantile float64) (CacheCommandResponse, bool) {
	estimate, ok, err := ht.EstimateQuantileSketchChecked(key, quantile)
	if err != nil {
		return commandError(err.Error()), true
	}
	if !ok {
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	return commandFastQuantileEstimateResponse("ok", estimate), true
}

func (ht *HatTrie) executeFastAddFenwickTreeCommand(key string, index uint64, delta int64) (CacheCommandResponse, bool) {
	update, ok, err := ht.AddFenwickTreeChecked(key, index, delta)
	if err != nil {
		return commandError(err.Error()), true
	}
	if !ok {
		return commandError("fenwick tree update is out of range or overflows"), true
	}
	return commandFastFenwickTreeUpdateResponse(update), true
}

func (ht *HatTrie) executeFastRangeFenwickTreeCommand(key string, start uint64, end uint64) (CacheCommandResponse, bool) {
	value, ok, err := ht.RangeSumFenwickTreeChecked(key, start, end)
	if err != nil {
		return commandError(err.Error()), true
	}
	if !ok {
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatInt(value, 10)}, true
}

func commandFastPathField(value string) bool {
	if value == "" {
		return false
	}
	first := value[0]
	last := value[len(value)-1]
	if first > ' ' && first < utf8.RuneSelf && last > ' ' && last < utf8.RuneSelf {
		return true
	}
	return strings.TrimSpace(value) == value
}

func commandFastJSONPlainString(value string) bool {
	if value == "" {
		return false
	}
	for idx := 0; idx < len(value); idx++ {
		c := value[idx]
		if c < 0x20 || c == '"' || c == '\\' || c >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

func commandFastPriorityQueueItemJSON(priority int64, value string) (string, bool) {
	if !commandFastJSONPlainString(value) {
		return "", false
	}
	var digits [20]byte
	priorityDigits := strconv.AppendInt(digits[:0], priority, 10)
	var builder strings.Builder
	builder.Grow(len(`{"priority":`) + len(priorityDigits) + len(`,"value":""}`) + len(value))
	builder.WriteString(`{"priority":`)
	_, _ = builder.Write(priorityDigits)
	builder.WriteString(`,"value":"`)
	builder.WriteString(value)
	builder.WriteString(`"}`)
	return builder.String(), true
}

func commandFastTopKEstimateResponse(message string, estimate TopKEstimate) CacheCommandResponse {
	var buffer [96]byte
	out := buffer[:0]
	out = append(out, `{"tracked":`...)
	out = strconv.AppendBool(out, estimate.Tracked)
	out = append(out, `,"count":`...)
	out = strconv.AppendUint(out, estimate.Count, 10)
	out = append(out, `,"error":`...)
	out = strconv.AppendUint(out, estimate.Error, 10)
	out = append(out, '}')
	return CacheCommandResponse{OK: true, Message: message, Value: string(out)}
}

func commandFastReservoirSampleUpdateResponse(message string, update ReservoirSampleUpdate) CacheCommandResponse {
	var buffer [96]byte
	out := buffer[:0]
	out = append(out, `{"accepted":`...)
	out = strconv.AppendBool(out, update.Accepted)
	out = append(out, `,"seen":`...)
	out = strconv.AppendUint(out, update.Seen, 10)
	out = append(out, `,"tracked":`...)
	out = strconv.AppendUint(out, update.Tracked, 10)
	out = append(out, `,"capacity":`...)
	out = strconv.AppendUint(out, update.Capacity, 10)
	out = append(out, '}')
	return CacheCommandResponse{OK: true, Message: message, Value: string(out)}
}

func commandFastTopKItemsJSON(top topKData) (string, bool) {
	if len(top.items) == 0 {
		return "[]", true
	}
	if len(top.items) != 1 {
		return "", false
	}
	item := top.items[0]
	value, ok := item.Value.(string)
	if !ok || !commandFastJSONPlainString(value) {
		return "", false
	}
	var builder strings.Builder
	builder.Grow(len(`[{"value":"","count":18446744073709551615,"error":18446744073709551615}]`) + len(value))
	builder.WriteString(`[{"value":"`)
	builder.WriteString(value)
	builder.WriteString(`","count":`)
	builder.WriteString(strconv.FormatUint(item.Count, 10))
	builder.WriteString(`,"error":`)
	builder.WriteString(strconv.FormatUint(item.Error, 10))
	builder.WriteString(`}]`)
	return builder.String(), true
}

func commandFastQuantileEstimateResponse(message string, estimate QuantileEstimate) CacheCommandResponse {
	var buffer [160]byte
	out := buffer[:0]
	out = append(out, `{"quantile":`...)
	out = strconv.AppendFloat(out, estimate.Quantile, 'g', -1, 64)
	out = append(out, `,"value":`...)
	out = strconv.AppendFloat(out, estimate.Value, 'g', -1, 64)
	out = append(out, `,"count":`...)
	out = strconv.AppendUint(out, estimate.Count, 10)
	out = append(out, `,"rank_error":`...)
	out = strconv.AppendUint(out, estimate.RankError, 10)
	out = append(out, '}')
	return CacheCommandResponse{OK: true, Message: message, Value: string(out)}
}

func commandFastFenwickTreeUpdateResponse(update FenwickTreeUpdate) CacheCommandResponse {
	var buffer [224]byte
	out := buffer[:0]
	out = append(out, `{"index":`...)
	out = strconv.AppendUint(out, update.Index, 10)
	out = append(out, `,"delta":`...)
	out = strconv.AppendInt(out, update.Delta, 10)
	out = append(out, `,"value":`...)
	out = strconv.AppendInt(out, update.Value, 10)
	out = append(out, `,"prefix_sum":`...)
	out = strconv.AppendInt(out, update.PrefixSum, 10)
	out = append(out, `,"total":`...)
	out = strconv.AppendInt(out, update.Total, 10)
	out = append(out, `,"updates":`...)
	out = strconv.AppendUint(out, update.Updates, 10)
	out = append(out, '}')
	return CacheCommandResponse{OK: true, Message: "updated fenwick tree", Value: string(out)}
}

func commandFastUint32Field(value string) (uint32, bool) {
	parsed, ok := commandFastUint64Field(value)
	if !ok || parsed > math.MaxUint32 {
		return 0, false
	}
	return uint32(parsed), true
}

func commandFastUint64Field(value string) (uint64, bool) {
	if value == "" {
		return 0, false
	}
	var parsed uint64
	for idx := 0; idx < len(value); idx++ {
		c := value[idx]
		if c < '0' || c > '9' {
			return 0, false
		}
		digit := uint64(c - '0')
		if parsed > (math.MaxUint64-digit)/10 {
			return 0, false
		}
		parsed = parsed*10 + digit
	}
	return parsed, true
}

func commandFastOptionalUint64Field(value string, defaultValue uint64) (uint64, bool) {
	if value == "" {
		return defaultValue, true
	}
	return commandFastUint64Field(value)
}

func commandFastInt64Field(value string) (int64, bool) {
	if value == "" || strings.TrimSpace(value) != value {
		return 0, false
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	return parsed, err == nil
}

func commandFastFloat64Field(value string) (float64, bool) {
	if value == "" || strings.TrimSpace(value) != value {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(value, 64)
	return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
}

func (ht *HatTrie) executeFastGetCommand(key string) (CacheCommandResponse, bool) {
	ht.mu.RLock()
	hval, fallback, err := ht.readValueRLockedChecked(key, true)
	if fallback || err != nil {
		ht.mu.RUnlock()
		return CacheCommandResponse{}, false
	}
	if hval.Empty() {
		ht.recordReadLocked(false, key)
		ht.mu.RUnlock()
		return CacheCommandResponse{OK: true, Message: "key not found"}, true
	}
	switch {
	case hval.IsStringAtRaws():
		value := ht.raws.stringValue(hval.Index)
		ht.recordReadLocked(true, key)
		ht.mu.RUnlock()
		return CacheCommandResponse{OK: true, Message: "ok", Value: value}, true
	case hval.IsCounter():
		value := strconv.FormatInt(int64(hval.Index), 10)
		ht.recordReadLocked(true, key)
		ht.mu.RUnlock()
		return CacheCommandResponse{OK: true, Message: "ok", Value: value}, true
	case hval.IsBytesAtRaws() && !hval.OnDisk():
		value := string(ht.raws.array[hval.Index])
		ht.recordReadLocked(true, key)
		ht.mu.RUnlock()
		return CacheCommandResponse{OK: true, Message: "ok", Value: value}, true
	default:
		ht.mu.RUnlock()
		return CacheCommandResponse{}, false
	}
}

func (ht *HatTrie) executeFastPeekMapCommand(key string, subkey string) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsMap() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	value, ok := ht.maps.array[hval.Index][subkey]
	ht.recordReadLocked(ok, key)
	if !ok {
		return CacheCommandResponse{OK: true, Message: "value not found"}, true
	}
	if text, ok := value.(string); ok {
		return CacheCommandResponse{OK: true, Message: "ok", Value: text}, true
	}
	payload, err := commandScalarString(value)
	if err != nil {
		return commandError(err.Error()), true
	}
	return CacheCommandResponse{OK: true, Message: "ok", Value: payload}, true
}

func (ht *HatTrie) executeFastHasRoaringBitmapCommand(key string, value uint32) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsRoaringBitmap() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}, true
	}
	hit := ht.roaringBitmaps.array[hval.Index].Contains(value)
	ht.recordReadLocked(hit, key)
	return commandBool01Response(hit), true
}

func (ht *HatTrie) executeFastHasSparseBitsetCommand(key string, value uint64) (CacheCommandResponse, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekCachedLocked(key)
	if !hval.IsSparseBitset() {
		if hval.IsLevelDBReference() {
			return CacheCommandResponse{}, false
		}
		ht.recordReadLocked(false, key)
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}, true
	}
	hit := ht.sparseBitsets.array[hval.Index].Contains(value)
	ht.recordReadLocked(hit, key)
	return commandBool01Response(hit), true
}

func commandBool01Response(hit bool) CacheCommandResponse {
	if hit {
		return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
	}
	return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
}

func (ht *HatTrie) commandIncrementCounter(key string, by int32) (int32, bool, error) {
	value, ok, err := ht.incrementCounterChecked(key, by, true)
	if err != nil {
		return 0, false, err
	}
	return value, ok, nil
}

func (ht *HatTrie) commandPutMap(key string, fields Map) error {
	return ht.PutMapEntriesChecked(key, fields)
}

func (ht *HatTrie) commandDumpEntry(key string) (string, bool, error) {
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.commandDumpEntry(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.commandDumpEntryLocked(key)
}

func (ht *HatTrie) commandDumpEntryLocked(key string) (string, bool, error) {
	hval := ht.peekCachedLocked(key)
	if hval.Empty() {
		ht.recordReadLocked(false, key)
		return "", false, nil
	}
	if hval.IsStringAtRaws() && ht.expirationTimeLocked(key).IsZero() {
		data := commandDumpStringJSON(key, ht.raws.stringValue(hval.Index))
		ht.recordReadLocked(true, key)
		return data, true, nil
	}
	entry, err := ht.snapshotEntryWithoutStatsLocked(Entry{Key: key, Value: hval})
	if err != nil {
		ht.recordReadLocked(false, key)
		return "", false, err
	}
	data, err := marshalSnapshotEntryJSON(entry)
	if err != nil {
		ht.recordReadLocked(false, key)
		return "", false, err
	}
	ht.recordReadLocked(true, key)
	return string(data), true, nil
}

func (ht *HatTrie) commandDumpEntryBinary(key string) ([]byte, bool, error) {
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.commandDumpEntryBinary(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	return ht.commandDumpEntryBinaryLocked(key)
}

func (ht *HatTrie) commandDumpEntryBinaryWithoutStats(key string) ([]byte, bool, error) {
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.commandDumpEntryBinaryWithoutStats(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval := ht.peekCachedLocked(key)
	if hval.Empty() {
		return nil, false, nil
	}
	return ht.commandDumpScannedEntryBinaryWithoutStatsLocked(Entry{Key: key, Value: hval})
}

func (ht *HatTrie) commandDumpEntryBinaryLocked(key string) ([]byte, bool, error) {
	hval := ht.peekCachedLocked(key)
	if hval.Empty() {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}
	return ht.commandDumpScannedEntryBinaryLocked(Entry{Key: key, Value: hval})
}

func (ht *HatTrie) commandDumpScannedEntryBinaryLocked(entry Entry) ([]byte, bool, error) {
	data, ok, err := ht.commandDumpScannedEntryBinaryWithoutStatsLocked(entry)
	ht.recordReadLocked(ok, entry.Key)
	return data, ok, err
}

func (ht *HatTrie) commandDumpScannedEntryBinaryWithoutStatsLocked(entry Entry) ([]byte, bool, error) {
	return ht.appendCommandDumpScannedEntryBinaryWithoutStatsLocked(nil, entry)
}

func (ht *HatTrie) appendCommandDumpScannedEntryBinaryWithoutStatsLocked(destination []byte, entry Entry) ([]byte, bool, error) {
	if partition := ht.localPartitionForKey(entry.Key); partition != nil {
		return partition.appendCommandDumpScannedEntryBinaryWithoutStatsLocked(destination, entry)
	}
	if entry.Value.Empty() {
		return destination, false, nil
	}
	if entry.Value.IsStringAtRaws() && ht.expirationTimeLocked(entry.Key).IsZero() {
		data, err := appendReplicationValueBinary(destination, snapshotEntry{Type: "string", String: ht.raws.stringValue(entry.Value.Index)})
		if err != nil {
			return destination, false, err
		}
		return data, true, nil
	}
	snapshot, err := ht.snapshotEntryWithoutStatsLocked(entry)
	if err != nil {
		return destination, false, err
	}
	data, err := appendReplicationValueBinary(destination, snapshot)
	if err != nil {
		return destination, false, err
	}
	return data, true, nil
}

func commandDumpStringJSON(key string, value string) string {
	var builder strings.Builder
	builder.Grow(len(key) + len(value) + 36)
	builder.WriteString(`{"key":`)
	writeCommandJSONString(&builder, key)
	builder.WriteString(`,"type":"string","string":`)
	writeCommandJSONString(&builder, value)
	builder.WriteByte('}')
	return builder.String()
}

func writeCommandJSONString(builder *strings.Builder, value string) {
	if commandStringNeedsJSONEscape(value) {
		builder.WriteString(strconv.Quote(value))
		return
	}
	builder.WriteByte('"')
	builder.WriteString(value)
	builder.WriteByte('"')
}

func commandStringNeedsJSONEscape(value string) bool {
	for idx := 0; idx < len(value); idx++ {
		c := value[idx]
		if c < 0x20 || c == '\\' || c == '"' || c >= utf8.RuneSelf {
			return true
		}
	}
	return false
}

func (ht *HatTrie) commandInternalSet(key string, payload string) error {
	operation, err := commandSnapshotOperation(key, payload)
	if err != nil {
		return err
	}
	return ht.commandInternalSetOperation(operation)
}

func (ht *HatTrie) commandInternalSetOperation(operation snapshotOperation) error {
	if partition := ht.localPartitionForKey(operation.entry.Key); partition != nil {
		return partition.commandInternalSetOperation(operation)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	_, err := ht.applySnapshotOperationLocked(operation)
	if err == nil {
		ht.recordWriteLocked(operation.entry.Key)
	}
	return err
}

func executePreparedInternalReplicationCommand(trie *HatTrie, request CacheCommandRequest, operation *snapshotOperation) CacheCommandResponse {
	if trie == nil {
		return commandError(ErrNilHatTrie.Error())
	}
	switch normalizedCommand(request.Command) {
	case "INTERNALSET", replicationSetBinaryCommand, replicationSetCompactCommand:
		if operation == nil {
			return commandError("prepared internal set operation is required")
		}
		if err := trie.commandInternalSetOperation(*operation); err != nil {
			return commandError(err.Error())
		}
		return CacheCommandResponse{OK: true, Message: "internal value stored"}
	case "INTERNALDEL":
		if trie.Delete(strings.TrimSpace(request.Key)) {
			return CacheCommandResponse{OK: true, Message: "internal value deleted"}
		}
		return CacheCommandResponse{OK: true, Message: "key not found"}
	default:
		return commandError("prepared internal replication command must be INTERNALSET or INTERNALDEL")
	}
}

func commandSnapshotBinaryOperation(key string, payload []byte) (snapshotOperation, error) {
	if len(payload) == 0 {
		return snapshotOperation{}, errors.New("binary snapshot entry is required")
	}
	entry, err := decodeLevelDBEntry(payload)
	if err != nil {
		return snapshotOperation{}, err
	}
	if entry.Key != key {
		return snapshotOperation{}, errors.New("snapshot entry key does not match request key")
	}
	return snapshotOperationForEntry(entry)
}

func commandReplicationValueOperation(key string, payload []byte) (snapshotOperation, error) {
	if len(payload) == 0 {
		return snapshotOperation{}, errors.New("binary replication value is required")
	}
	entry, err := unmarshalReplicationValueBinary(key, payload)
	if err != nil {
		return snapshotOperation{}, err
	}
	return snapshotOperationForEntry(entry)
}

func commandSnapshotOperation(key string, payload string) (snapshotOperation, error) {
	if strings.TrimSpace(payload) == "" {
		return snapshotOperation{}, errors.New("snapshot entry JSON is required")
	}
	data := []byte(payload)
	hasKey, err := snapshotEntryHasKey(data)
	if err != nil {
		return snapshotOperation{}, err
	}
	entry, err := decodeSnapshotEntryJSON(data)
	if err != nil {
		return snapshotOperation{}, err
	}
	if !hasKey {
		entry.Key = key
	} else if entry.Key != key {
		return snapshotOperation{}, errors.New("snapshot entry key does not match request key")
	}
	return snapshotOperationForEntry(entry)
}

func (ht *HatTrie) applyCommandExpiration(key string, ttlSeconds *int64, unixSeconds *int64) (CacheCommandResponse, bool) {
	if ttlSeconds == nil && unixSeconds == nil {
		return CacheCommandResponse{}, false
	}
	if ttlSeconds != nil && unixSeconds != nil {
		return commandError("ttl_seconds and unix_seconds are mutually exclusive"), true
	}
	if ttlSeconds != nil {
		ttl, ok := requirePositiveTTL(ttlSeconds)
		if !ok {
			return commandError("ttl_seconds must be positive"), true
		}
		if !ht.Expire(key, ttl) {
			return commandError("failed to set ttl"), true
		}
		return CacheCommandResponse{OK: true, Message: "stored with ttl"}, true
	}
	if !ht.ExpireAt(key, time.Unix(*unixSeconds, 0)) {
		return commandError("failed to set expiration"), true
	}
	return CacheCommandResponse{OK: true, Message: "stored with expiration"}, true
}

func validateOptionalCommandExpiration(ttlSeconds *int64, unixSeconds *int64) (CacheCommandResponse, bool) {
	if ttlSeconds == nil && unixSeconds == nil {
		return CacheCommandResponse{}, false
	}
	if ttlSeconds != nil && unixSeconds != nil {
		return commandError("ttl_seconds and unix_seconds are mutually exclusive"), true
	}
	if unixSeconds != nil {
		return CacheCommandResponse{OK: true}, true
	}
	if _, ok := requirePositiveTTL(ttlSeconds); !ok {
		return commandError("ttl_seconds must be positive"), true
	}
	return CacheCommandResponse{OK: true}, true
}

func (ht *HatTrie) commandValue(key string) (string, bool, error) {
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.commandValue(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	return ht.commandValueLockedForKey(key)
}

func (ht *HatTrie) commandValueLockedForKey(key string) (string, bool, error) {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return "", false, err
	}
	if hval.Empty() {
		ht.recordReadLocked(false, key)
		return "", false, nil
	}

	value, err := ht.commandValueLocked(hval)
	if err != nil {
		ht.recordReadLocked(false, key)
		return "", false, err
	}
	ht.recordReadLocked(true, key)
	return value, true, nil
}

func (ht *HatTrie) upsertStringLocked(key string, value string) error {
	if err := ht.upsertStringValueLocked(key, value); err != nil {
		return err
	}
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) upsertStringValueLocked(key string, value string) error {
	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsStringAtRaws() {
		ht.raws.putStringOwned(hval.Index, value)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.raws.addStringOwned(value)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
	return nil
}

func (ht *HatTrie) upsertCounterLocked(key string, value int32) error {
	if err := ht.upsertCounterValueLocked(key, value); err != nil {
		return err
	}
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) upsertCounterValueLocked(key string, value int32) error {
	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if !hval.IsCounter() {
		ht.returnStorage(hval)
	}
	ht.clearExpirationLocked(key)
	*rawPtr = HatValue{Index: value, Flags: DATAVALUE_TYPE_COUNTER}.toValue()
	return nil
}

func (ht *HatTrie) incrementCounterLocked(key string, by int32, checkOverflow bool) (int32, bool, error) {
	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return 0, false, err
	}
	if hval.IsCounter() {
		if checkOverflow {
			next := int64(hval.Index) + int64(by)
			if next < minCommandInt32 || next > maxCommandInt32 {
				return hval.Index, false, nil
			}
			hval.Index = int32(next)
		} else {
			hval.Index += by
		}
	} else {
		if rawPtr == nil {
			rawPtr = ht.upsertLocation(key)
		}
		ht.returnStorage(hval)
		ht.clearExpirationLocked(key)
		hval.Flags = DATAVALUE_TYPE_COUNTER
		hval.Index = by
	}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	return hval.Index, true, nil
}

func (ht *HatTrie) deleteAndRecordLocked(key string) bool {
	deleted := ht.deleteLocked(key)
	if deleted {
		ht.recordDeleteLocked(key)
	}
	return deleted
}

func (ht *HatTrie) expireRelativeLocked(key string, ttl time.Duration) bool {
	if ttl <= 0 {
		return ht.deleteAndRecordLocked(key)
	}
	return ht.expireAtAndRecordLocked(key, ht.currentTime().Add(ttl))
}

func (ht *HatTrie) expireAtAndRecordLocked(key string, at time.Time) bool {
	ok, deleted := ht.expireAtLocked(key, at)
	if ok {
		if deleted {
			ht.recordDeleteLocked(key)
		} else {
			ht.recordWriteLocked(key)
		}
	}
	return ok
}

func (ht *HatTrie) persistLocked(key string) bool {
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		ht.clearExpirationLocked(key)
		return false
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return false
	}
	if _, ok := ht.expires[key]; !ok {
		return false
	}

	ht.clearExpirationLocked(key)
	hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	return true
}

func (ht *HatTrie) ttlLocked(key string) time.Duration {
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		ht.clearExpirationLocked(key)
		ht.recordReadLocked(false, key)
		return NoTTL
	}

	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		ht.recordReadLocked(false, key)
		return NoTTL
	}

	expiresAt, ok := ht.expirationAtLocked(key)
	if !ok {
		ht.recordReadLocked(false, key)
		return NoTTL
	}
	ttl := expiresAt.Sub(ht.currentTime())
	if ttl <= 0 {
		if ht.deleteKnownLocked(key, hval) {
			ht.recordExpirationLocked(key)
		}
		ht.recordReadLocked(false, key)
		return NoTTL
	}
	ht.recordReadLocked(true, key)
	return ttl
}

func (ht *HatTrie) applyCommandExpirationLocked(key string, ttlSeconds *int64, unixSeconds *int64) (CacheCommandResponse, bool) {
	if ttlSeconds == nil && unixSeconds == nil {
		return CacheCommandResponse{}, false
	}
	if ttlSeconds != nil && unixSeconds != nil {
		return commandError("ttl_seconds and unix_seconds are mutually exclusive"), true
	}
	if ttlSeconds != nil {
		ttl, ok := requirePositiveTTL(ttlSeconds)
		if !ok {
			return commandError("ttl_seconds must be positive"), true
		}
		if !ht.expireRelativeLocked(key, ttl) {
			return commandError("failed to set ttl"), true
		}
		return CacheCommandResponse{OK: true, Message: "stored with ttl"}, true
	}
	if !ht.expireAtAndRecordLocked(key, time.Unix(*unixSeconds, 0)) {
		return commandError("failed to set expiration"), true
	}
	return CacheCommandResponse{OK: true, Message: "stored with expiration"}, true
}

func (ht *HatTrie) commandValueLocked(hval HatValue) (string, error) {
	switch hval.Type() {
	case DATAVALUE_TYPE_COUNTER:
		return strconv.FormatInt(int64(hval.Index), 10), nil
	case DATAVALUE_TYPE_RAW_STRING:
		return ht.raws.stringValue(hval.Index), nil
	case DATAVALUE_TYPE_RAW_BYTES:
		var value []byte
		if hval.OnDisk() {
			bytes, err := ht.disks.Get(hval.Index)
			if err != nil {
				return "", err
			}
			value = bytes
		} else {
			value = ht.raws.array[hval.Index]
		}
		return string(value), nil
	case DATAVALUE_TYPE_MAP:
		return jsonEncodedString(ht.maps.array[hval.Index])
	case DATAVALUE_TYPE_SLICE:
		return jsonEncodedString(ht.slices.array[hval.Index].Slice())
	case DATAVALUE_TYPE_SET:
		return jsonEncodedString(ht.sets.array[hval.Index].Values())
	case DATAVALUE_TYPE_PRIORITY_QUEUE:
		return jsonEncodedString(ht.priorityQueues.array[hval.Index].Items())
	case DATAVALUE_TYPE_BLOOM_FILTER:
		return jsonEncodedString(ht.bloomFilters.array[hval.Index].Info())
	case DATAVALUE_TYPE_CUCKOO_FILTER:
		return jsonEncodedString(ht.cuckooFilters.array[hval.Index].Info())
	case DATAVALUE_TYPE_ROARING_BITMAP:
		return jsonEncodedString(ht.roaringBitmaps.array[hval.Index].Info())
	case DATAVALUE_TYPE_SPARSE_BITSET:
		return jsonEncodedString(ht.sparseBitsets.array[hval.Index].Info())
	case DATAVALUE_TYPE_COUNT_MIN_SKETCH:
		return jsonEncodedString(ht.countMinSketches.array[hval.Index].Info())
	case DATAVALUE_TYPE_HYPERLOGLOG:
		return jsonEncodedString(ht.hyperLogLogs.array[hval.Index].Info())
	case DATAVALUE_TYPE_TOP_K:
		return jsonEncodedString(ht.topKs.array[hval.Index].Items())
	case DATAVALUE_TYPE_RESERVOIR_SAMPLE:
		return jsonEncodedString(ht.reservoirSamples.array[hval.Index].Items())
	case DATAVALUE_TYPE_XOR_FILTER:
		return jsonEncodedString(ht.xorFilters.array[hval.Index].Info())
	case DATAVALUE_TYPE_RADIX_TREE:
		return jsonEncodedString(ht.radixTrees.array[hval.Index].Info())
	case DATAVALUE_TYPE_QUANTILE_SKETCH:
		return jsonEncodedString(ht.quantileSketches.array[hval.Index].Info())
	case DATAVALUE_TYPE_FENWICK_TREE:
		return jsonEncodedString(ht.fenwickTrees.array[hval.Index].Info())
	default:
		return "", nil
	}
}

func parseCommandInt32(value string) (int32, bool) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(parsed), true
}

const (
	minCommandInt32 = -1 << 31
	maxCommandInt32 = 1<<31 - 1
)

func parseCommandIncrement(value string) (int32, bool) {
	if strings.TrimSpace(value) == "" {
		return 1, true
	}
	return parseCommandInt32(value)
}

func commandExpireAt(request CacheCommandRequest) (time.Time, bool) {
	var unixSeconds int64
	if request.UnixSeconds != nil {
		unixSeconds = *request.UnixSeconds
	} else {
		parsed, err := strconv.ParseInt(strings.TrimSpace(request.Value), 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		unixSeconds = parsed
	}
	return time.Unix(unixSeconds, 0), true
}

func commandMapFields(request CacheCommandRequest) (Map, bool) {
	fields := Map{}
	for subkey, value := range request.Pairs {
		if strings.TrimSpace(subkey) == "" {
			return nil, false
		}
		fields[subkey] = value
	}
	subkey := strings.TrimSpace(request.Subkey)
	if subkey != "" {
		fields[subkey] = request.Value
	}
	if len(fields) == 0 {
		return nil, false
	}
	return fields, true
}

func commandRadixTreeFields(request CacheCommandRequest) (Map, bool) {
	fields := Map{}
	for subkey, value := range request.Pairs {
		if strings.TrimSpace(subkey) == "" {
			return nil, false
		}
		fields[subkey] = value
	}
	subkey := strings.TrimSpace(request.Subkey)
	if subkey != "" {
		fields[subkey] = request.Value
	}
	if len(fields) == 0 {
		return nil, false
	}
	return fields, true
}

func commandRadixTreeSubkey(request CacheCommandRequest) (string, bool) {
	subkey := strings.TrimSpace(request.Subkey)
	if subkey == "" {
		return "", false
	}
	return subkey, true
}

func commandSliceValues(request CacheCommandRequest) (Slice, bool) {
	if len(request.Values) > 0 {
		return request.Values, true
	}
	if request.Value != "" {
		return Slice{request.Value}, true
	}
	return nil, false
}

func commandPriority(request CacheCommandRequest) (int64, bool) {
	if request.Priority != nil {
		return *request.Priority, true
	}
	if strings.TrimSpace(request.Subkey) == "" {
		return 0, false
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(request.Subkey), 10, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

func commandBloomFilterConfig(request CacheCommandRequest) (uint64, float64, error) {
	expectedItems := DefaultBloomFilterExpectedItems
	falsePositiveRate := DefaultBloomFilterFalsePositiveRate
	var err error

	if strings.TrimSpace(request.Value) != "" {
		expectedItems, err = strconv.ParseUint(strings.TrimSpace(request.Value), 10, 64)
		if err != nil {
			return 0, 0, errors.New("bloom filter expected items must be an unsigned integer")
		}
	}
	if strings.TrimSpace(request.Subkey) != "" {
		falsePositiveRate, err = strconv.ParseFloat(strings.TrimSpace(request.Subkey), 64)
		if err != nil {
			return 0, 0, errors.New("bloom filter false positive rate must be a number")
		}
	}
	for _, key := range []string{"expected_items", "expected", "items"} {
		if value, ok := request.Pairs[key]; ok {
			expectedItems, err = commandUint64Value(value)
			if err != nil {
				return 0, 0, errors.New("bloom filter expected items must be an unsigned integer")
			}
			break
		}
	}
	for _, key := range []string{"false_positive_rate", "fpr"} {
		if value, ok := request.Pairs[key]; ok {
			falsePositiveRate, err = commandFloat64Value(value)
			if err != nil {
				return 0, 0, errors.New("bloom filter false positive rate must be a number")
			}
			break
		}
	}
	return expectedItems, falsePositiveRate, nil
}

func commandCuckooFilterConfig(request CacheCommandRequest) (uint64, float64, error) {
	capacity := DefaultCuckooFilterCapacity
	falsePositiveRate := DefaultCuckooFilterFalsePositiveRate
	var err error

	if strings.TrimSpace(request.Value) != "" {
		capacity, err = strconv.ParseUint(strings.TrimSpace(request.Value), 10, 64)
		if err != nil {
			return 0, 0, errors.New("cuckoo filter capacity must be an unsigned integer")
		}
	}
	if strings.TrimSpace(request.Subkey) != "" {
		falsePositiveRate, err = strconv.ParseFloat(strings.TrimSpace(request.Subkey), 64)
		if err != nil {
			return 0, 0, errors.New("cuckoo filter false positive rate must be a number")
		}
	}
	for _, key := range []string{"capacity", "expected_items", "expected", "items"} {
		if value, ok := request.Pairs[key]; ok {
			capacity, err = commandUint64Value(value)
			if err != nil {
				return 0, 0, errors.New("cuckoo filter capacity must be an unsigned integer")
			}
			break
		}
	}
	for _, key := range []string{"false_positive_rate", "fpr"} {
		if value, ok := request.Pairs[key]; ok {
			falsePositiveRate, err = commandFloat64Value(value)
			if err != nil {
				return 0, 0, errors.New("cuckoo filter false positive rate must be a number")
			}
			break
		}
	}
	return capacity, falsePositiveRate, nil
}

func commandXorFilterExpectedItems(request CacheCommandRequest) (uint64, error) {
	expectedItems := DefaultXorFilterExpectedItems
	var err error
	if strings.TrimSpace(request.Value) != "" {
		expectedItems, err = strconv.ParseUint(strings.TrimSpace(request.Value), 10, 64)
		if err != nil {
			return 0, errors.New("xor filter expected items must be an unsigned integer")
		}
	}
	for _, key := range []string{"expected_items", "capacity", "expected", "items"} {
		if value, ok := request.Pairs[key]; ok {
			expectedItems, err = commandUint64Value(value)
			if err != nil {
				return 0, errors.New("xor filter expected items must be an unsigned integer")
			}
			break
		}
	}
	return expectedItems, validateXorFilterExpectedItems(expectedItems)
}

func commandCountMinSketchConfig(request CacheCommandRequest) (uint64, uint8, error) {
	width := DefaultCountMinSketchWidth
	depth := DefaultCountMinSketchDepth
	var err error

	if strings.TrimSpace(request.Value) != "" {
		width, err = strconv.ParseUint(strings.TrimSpace(request.Value), 10, 64)
		if err != nil {
			return 0, 0, errors.New("count-min sketch width must be an unsigned integer")
		}
	}
	if strings.TrimSpace(request.Subkey) != "" {
		parsed, err := strconv.ParseUint(strings.TrimSpace(request.Subkey), 10, 64)
		if err != nil {
			return 0, 0, errors.New("count-min sketch depth must be an unsigned integer")
		}
		depth, err = countMinSketchDepthValue(parsed)
		if err != nil {
			return 0, 0, err
		}
	}
	if value, ok := request.Pairs["width"]; ok {
		width, err = commandUint64Value(value)
		if err != nil {
			return 0, 0, errors.New("count-min sketch width must be an unsigned integer")
		}
	}
	if value, ok := request.Pairs["depth"]; ok {
		parsed, err := commandUint64Value(value)
		if err != nil {
			return 0, 0, errors.New("count-min sketch depth must be an unsigned integer")
		}
		depth, err = countMinSketchDepthValue(parsed)
		if err != nil {
			return 0, 0, err
		}
	}
	if err := validateCountMinSketchShape(width, depth); err != nil {
		return 0, 0, err
	}
	return width, depth, nil
}

func commandCountMinSketchIncrement(request CacheCommandRequest) (uint32, error) {
	count := uint64(1)
	if strings.TrimSpace(request.Subkey) != "" {
		parsed, err := strconv.ParseUint(strings.TrimSpace(request.Subkey), 10, 64)
		if err != nil {
			return 0, errors.New("count-min sketch increment must be an unsigned integer")
		}
		count = parsed
	}
	if value, ok := request.Pairs["count"]; ok {
		parsed, err := commandUint64Value(value)
		if err != nil {
			return 0, errors.New("count-min sketch increment must be an unsigned integer")
		}
		count = parsed
	}
	if count == 0 || count > uint64(maxCountMinSketchCounter) {
		return 0, errors.New("count-min sketch increment must be between 1 and " + strconv.FormatUint(uint64(maxCountMinSketchCounter), 10))
	}
	return uint32(count), nil
}

func commandHyperLogLogPrecision(request CacheCommandRequest) (uint8, error) {
	precision := DefaultHyperLogLogPrecision
	if strings.TrimSpace(request.Value) != "" {
		parsed, err := strconv.ParseUint(strings.TrimSpace(request.Value), 10, 64)
		if err != nil {
			return 0, errors.New("hyperloglog precision must be an unsigned integer")
		}
		next, err := hyperLogLogPrecisionValue(parsed)
		if err != nil {
			return 0, err
		}
		precision = next
	}
	if value, ok := request.Pairs["precision"]; ok {
		parsed, err := commandUint64Value(value)
		if err != nil {
			return 0, errors.New("hyperloglog precision must be an unsigned integer")
		}
		next, err := hyperLogLogPrecisionValue(parsed)
		if err != nil {
			return 0, err
		}
		precision = next
	}
	return precision, validateHyperLogLogPrecision(precision)
}

func hyperLogLogPrecisionValue(value uint64) (uint8, error) {
	if value < uint64(minHyperLogLogPrecision) || value > uint64(maxHyperLogLogPrecision) || value > uint64(^uint8(0)) {
		return 0, errors.New("hatriecache: hyperloglog precision must be between " + strconv.Itoa(int(minHyperLogLogPrecision)) + " and " + strconv.Itoa(int(maxHyperLogLogPrecision)))
	}
	return uint8(value), nil
}

func commandTopKCapacity(request CacheCommandRequest) (uint64, error) {
	capacity := DefaultTopKCapacity
	var err error
	if strings.TrimSpace(request.Value) != "" {
		capacity, err = strconv.ParseUint(strings.TrimSpace(request.Value), 10, 64)
		if err != nil {
			return 0, errors.New("top-k capacity must be an unsigned integer")
		}
	}
	if value, ok := request.Pairs["capacity"]; ok {
		capacity, err = commandUint64Value(value)
		if err != nil {
			return 0, errors.New("top-k capacity must be an unsigned integer")
		}
	}
	return topKCapacityValue(capacity)
}

func commandTopKCount(request CacheCommandRequest) (uint64, error) {
	count := uint64(1)
	var err error
	if strings.TrimSpace(request.Subkey) != "" {
		count, err = strconv.ParseUint(strings.TrimSpace(request.Subkey), 10, 64)
		if err != nil {
			return 0, errors.New("top-k count must be an unsigned integer")
		}
	}
	if value, ok := request.Pairs["count"]; ok {
		count, err = commandUint64Value(value)
		if err != nil {
			return 0, errors.New("top-k count must be an unsigned integer")
		}
	}
	if count == 0 {
		return 0, errors.New("top-k count must be positive")
	}
	return count, nil
}

func commandReservoirSampleCapacity(request CacheCommandRequest) (uint64, error) {
	capacity := DefaultReservoirSampleCapacity
	var err error
	if strings.TrimSpace(request.Value) != "" {
		capacity, err = strconv.ParseUint(strings.TrimSpace(request.Value), 10, 64)
		if err != nil {
			return 0, errors.New("reservoir sample capacity must be an unsigned integer")
		}
	}
	if value, ok := request.Pairs["capacity"]; ok {
		capacity, err = commandUint64Value(value)
		if err != nil {
			return 0, errors.New("reservoir sample capacity must be an unsigned integer")
		}
	}
	return reservoirSampleCapacityValue(capacity)
}

func commandQuantileSketchEpsilon(request CacheCommandRequest) (float64, error) {
	epsilon := DefaultQuantileSketchEpsilon
	var err error
	if strings.TrimSpace(request.Value) != "" {
		epsilon, err = strconv.ParseFloat(strings.TrimSpace(request.Value), 64)
		if err != nil {
			return 0, errors.New("quantile sketch epsilon must be a number")
		}
	}
	for _, key := range []string{"epsilon", "error"} {
		if value, ok := request.Pairs[key]; ok {
			epsilon, err = commandFloat64Value(value)
			if err != nil {
				return 0, errors.New("quantile sketch epsilon must be a number")
			}
			break
		}
	}
	return quantileSketchEpsilonValue(epsilon)
}

func quantileSketchValuesFromCommand(request CacheCommandRequest) ([]float64, error) {
	values, ok := commandSliceValues(request)
	if !ok {
		return nil, errors.New("value or values is required")
	}
	out := make([]float64, len(values))
	for idx, value := range values {
		parsed, err := commandFloat64Value(value)
		if err != nil || !validQuantileSketchValue(parsed) {
			return nil, errors.New("quantile sketch value must be a finite number")
		}
		out[idx] = parsed
	}
	return out, nil
}

func commandQuantileValue(request CacheCommandRequest) (float64, error) {
	value := strings.TrimSpace(request.Value)
	if strings.TrimSpace(request.Subkey) != "" {
		value = strings.TrimSpace(request.Subkey)
	}
	quantile := math.NaN()
	var err error
	if value != "" {
		quantile, err = strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, errors.New("quantile must be a number")
		}
	}
	for _, key := range []string{"quantile", "q"} {
		if pairValue, ok := request.Pairs[key]; ok {
			quantile, err = commandFloat64Value(pairValue)
			if err != nil {
				return 0, errors.New("quantile must be a number")
			}
			break
		}
	}
	if math.IsNaN(quantile) || math.IsInf(quantile, 0) || quantile < 0 || quantile > 1 {
		return 0, errors.New("quantile must be between 0 and 1")
	}
	return quantile, nil
}

func commandFenwickTreeSize(request CacheCommandRequest) (uint64, error) {
	size := DefaultFenwickTreeSize
	var err error
	if strings.TrimSpace(request.Value) != "" {
		size, err = strconv.ParseUint(strings.TrimSpace(request.Value), 10, 64)
		if err != nil {
			return 0, errors.New("fenwick tree size must be an unsigned integer")
		}
	}
	for _, key := range []string{"size", "capacity"} {
		if value, ok := request.Pairs[key]; ok {
			size, err = commandUint64Value(value)
			if err != nil {
				return 0, errors.New("fenwick tree size must be an unsigned integer")
			}
			break
		}
	}
	if err := validateFenwickTreeSize(size); err != nil {
		return 0, err
	}
	return size, nil
}

func commandFenwickTreeUpdate(request CacheCommandRequest) (uint64, int64, error) {
	indexValue := interface{}(strings.TrimSpace(request.Value))
	deltaValue := interface{}(strings.TrimSpace(request.Subkey))
	if len(request.Values) > 0 {
		indexValue = request.Values[0]
	}
	if len(request.Values) > 1 {
		deltaValue = request.Values[1]
	}
	if value, ok := request.Pairs["index"]; ok {
		indexValue = value
	}
	if value, ok := request.Pairs["delta"]; ok {
		deltaValue = value
	}
	index, err := commandFenwickTreeIndexValue(indexValue)
	if err != nil {
		return 0, 0, err
	}
	delta, err := commandFenwickTreeDeltaValue(deltaValue)
	if err != nil {
		return 0, 0, err
	}
	return index, delta, nil
}

func commandFenwickTreeIndex(request CacheCommandRequest) (uint64, error) {
	value := interface{}(strings.TrimSpace(request.Value))
	if strings.TrimSpace(request.Subkey) != "" {
		value = strings.TrimSpace(request.Subkey)
	}
	if len(request.Values) > 0 {
		value = request.Values[0]
	}
	for _, key := range []string{"index", "end"} {
		if pairValue, ok := request.Pairs[key]; ok {
			value = pairValue
			break
		}
	}
	return commandFenwickTreeIndexValue(value)
}

func commandFenwickTreeRange(request CacheCommandRequest) (uint64, uint64, error) {
	startValue := interface{}(strings.TrimSpace(request.Value))
	endValue := interface{}(strings.TrimSpace(request.Subkey))
	if len(request.Values) > 0 {
		startValue = request.Values[0]
	}
	if len(request.Values) > 1 {
		endValue = request.Values[1]
	}
	if value, ok := request.Pairs["start"]; ok {
		startValue = value
	}
	if value, ok := request.Pairs["end"]; ok {
		endValue = value
	}
	start, err := commandFenwickTreeIndexValue(startValue)
	if err != nil {
		return 0, 0, errors.New("fenwick tree range start must be an unsigned integer")
	}
	end, err := commandFenwickTreeIndexValue(endValue)
	if err != nil {
		return 0, 0, errors.New("fenwick tree range end must be an unsigned integer")
	}
	if start > end {
		return 0, 0, errors.New("fenwick tree range start must be less than or equal to end")
	}
	return start, end, nil
}

func commandFenwickTreeIndexValue(value interface{}) (uint64, error) {
	index, err := commandUint64Value(value)
	if err != nil {
		return 0, errors.New("fenwick tree index must be an unsigned integer")
	}
	return index, nil
}

func commandFenwickTreeDeltaValue(value interface{}) (int64, error) {
	delta, err := commandInt64Value(value)
	if err != nil {
		return 0, errors.New("fenwick tree delta must be a 64-bit integer")
	}
	if delta == 0 {
		return 0, errors.New("fenwick tree delta must be non-zero")
	}
	return delta, nil
}

func commandUint64Value(value interface{}) (uint64, error) {
	switch typed := value.(type) {
	case json.Number:
		return strconv.ParseUint(typed.String(), 10, 64)
	case string:
		return strconv.ParseUint(strings.TrimSpace(typed), 10, 64)
	case uint64:
		return typed, nil
	case uint:
		return uint64(typed), nil
	case uint32:
		return uint64(typed), nil
	case int:
		if typed < 0 {
			return 0, errors.New("negative value")
		}
		return uint64(typed), nil
	case int64:
		if typed < 0 {
			return 0, errors.New("negative value")
		}
		return uint64(typed), nil
	case int32:
		if typed < 0 {
			return 0, errors.New("negative value")
		}
		return uint64(typed), nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return 0, errors.New("invalid number")
		}
		if typed >= float64(^uint64(0)) {
			return 0, errors.New("value too large")
		}
		converted := uint64(typed)
		if typed < 0 || float64(converted) != typed {
			return 0, errors.New("non-integer value")
		}
		return converted, nil
	default:
		return 0, errors.New("unsupported numeric value")
	}
}

func commandInt64Value(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case json.Number:
		return strconv.ParseInt(typed.String(), 10, 64)
	case string:
		return strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
	case int64:
		return typed, nil
	case int:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case uint64:
		if typed > uint64(maxFenwickTreeInt64) {
			return 0, errors.New("value too large")
		}
		return int64(typed), nil
	case uint:
		if uint64(typed) > uint64(maxFenwickTreeInt64) {
			return 0, errors.New("value too large")
		}
		return int64(typed), nil
	case uint32:
		return int64(typed), nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return 0, errors.New("invalid number")
		}
		if typed < float64(minFenwickTreeInt64) || typed >= float64(maxFenwickTreeInt64) {
			return 0, errors.New("value too large")
		}
		converted := int64(typed)
		if float64(converted) != typed {
			return 0, errors.New("non-integer value")
		}
		return converted, nil
	default:
		return 0, errors.New("unsupported numeric value")
	}
}

func commandFloat64Value(value interface{}) (float64, error) {
	switch typed := value.(type) {
	case json.Number:
		return strconv.ParseFloat(typed.String(), 64)
	case string:
		return strconv.ParseFloat(strings.TrimSpace(typed), 64)
	case float64:
		return typed, nil
	case float32:
		return float64(typed), nil
	case uint64:
		return float64(typed), nil
	case uint:
		return float64(typed), nil
	case uint32:
		return float64(typed), nil
	case int:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case int32:
		return float64(typed), nil
	default:
		return 0, errors.New("unsupported numeric value")
	}
}

func commandValueResponse(message string, value interface{}) CacheCommandResponse {
	payload, err := commandScalarString(value)
	if err != nil {
		return commandError(err.Error())
	}
	return CacheCommandResponse{OK: true, Message: message, Value: payload}
}

func commandScalarString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case json.Number:
		return v.String(), nil
	default:
		return jsonEncodedString(v)
	}
}

func requirePositiveTTL(ttlSeconds *int64) (time.Duration, bool) {
	if ttlSeconds == nil || *ttlSeconds <= 0 || *ttlSeconds > maxCommandTTLSeconds {
		return 0, false
	}
	return time.Duration(*ttlSeconds) * time.Second, true
}

func commandError(message string) CacheCommandResponse {
	return CacheCommandResponse{OK: false, Message: message}
}
