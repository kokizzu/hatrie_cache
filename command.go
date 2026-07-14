package hatriecache

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
)

const maxCommandTTLSeconds = int64(1<<63-1) / int64(time.Second)

type CacheCommandRequest struct {
	Command     string `json:"command"`
	Key         string `json:"key"`
	Value       string `json:"value,omitempty"`
	Values      Slice  `json:"values,omitempty"`
	Subkey      string `json:"subkey,omitempty"`
	Pairs       Map    `json:"pairs,omitempty"`
	Priority    *int64 `json:"priority,omitempty"`
	TTLSeconds  *int64 `json:"ttl_seconds,omitempty"`
	UnixSeconds *int64 `json:"unix_seconds,omitempty"`
}

type CacheCommandResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
	Value   string `json:"value,omitempty"`
}

func (ht *HatTrie) ExecuteCommand(request CacheCommandRequest) CacheCommandResponse {
	command := strings.ToUpper(strings.TrimSpace(request.Command))
	key := strings.TrimSpace(request.Key)
	if command == "" {
		return commandError("command is required")
	}
	if key == "" {
		return commandError("key is required")
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
		ht.UpsertString(key, request.Value)
		if response, ok := ht.applyCommandExpiration(key, request.TTLSeconds, request.UnixSeconds); ok {
			return response
		}
		return CacheCommandResponse{OK: true, Message: "stored string"}
	case "SETX", "SETSTRX":
		ttl, ok := requirePositiveTTL(request.TTLSeconds)
		if !ok {
			return commandError("positive ttl_seconds is required")
		}
		ht.UpsertString(key, request.Value)
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
		ht.UpsertCounter(key, value)
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
		ht.UpsertCounter(key, value)
		if !ht.Expire(key, ttl) {
			return commandError("failed to set ttl")
		}
		return CacheCommandResponse{OK: true, Message: "stored counter with ttl"}
	case "INC":
		by, ok := parseCommandIncrement(request.Value)
		if !ok {
			return commandError("value must be a 32-bit integer")
		}
		value, ok := ht.commandIncrementCounter(key, by)
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
		ht.commandPutMap(key, fields)
		return CacheCommandResponse{OK: true, Message: "stored map fields"}
	case "PEEKMAP":
		subkey := strings.TrimSpace(request.Subkey)
		if subkey == "" {
			return commandError("subkey is required")
		}
		value := ht.PeekMap(key, subkey)
		if value == nil {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "TAKEMAP":
		subkey := strings.TrimSpace(request.Subkey)
		if subkey == "" {
			return commandError("subkey is required")
		}
		value := ht.TakeMap(key, subkey)
		if value == nil {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("removed", value)
	case "PUSHSLICE":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		ht.PushSlice(key, values[0], values[1:]...)
		return CacheCommandResponse{OK: true, Message: "pushed slice values"}
	case "POPSLICE":
		value := ht.PopSlice(key)
		if value == nil {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("removed", value)
	case "SHIFTSLICE":
		value := ht.ShiftSlice(key)
		if value == nil {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("removed", value)
	case "HEADSLICE":
		value := ht.HeadSlice(key)
		if value == nil {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "TAILSLICE":
		value := ht.TailSlice(key)
		if value == nil {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "ADDSET":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		added := ht.AddSet(key, values[0], values[1:]...)
		return CacheCommandResponse{OK: true, Message: "added set values", Value: strconv.Itoa(added)}
	case "REMSET":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		removed := ht.RemoveSet(key, values[0], values[1:]...)
		return CacheCommandResponse{OK: true, Message: "removed set values", Value: strconv.Itoa(removed)}
	case "HASSET":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value or values is required")
		}
		if ht.HasSet(key, values[0]) {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "GETSET":
		value := ht.GetSet(key)
		if value == nil {
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
		added := ht.PushPriorityQueue(key, priority, values[0], values[1:]...)
		return CacheCommandResponse{OK: true, Message: "pushed priority queue values", Value: strconv.Itoa(added)}
	case "PEEKPQ", "PEEKPRIORITY":
		value, ok := ht.PeekPriorityQueue(key)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "POPPQ", "POPPRIORITY":
		value, ok := ht.PopPriorityQueue(key)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("removed", value)
	case "GETPQ", "GETPRIORITY":
		value := ht.GetPriorityQueue(key)
		if value == nil {
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
		info, ok := ht.BloomFilterInfo(key)
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
		info, ok := ht.CuckooFilterInfo(key)
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
		added, err := ht.AddXorFilter(key, values[0], values[1:]...)
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
		info, ok := ht.XorFilterInfo(key)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		if !info.Built {
			return commandError("xor filter is not built")
		}
		hit, queryable := ht.HasXorFilter(key, values[0])
		if !queryable {
			return commandError("xor filter is not built")
		}
		if hit {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "INFOXF", "XFINFO":
		info, ok := ht.XorFilterInfo(key)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATERB", "CREATEROARING", "RBRESERVE":
		ht.UpsertRoaringBitmap(key)
		return CacheCommandResponse{OK: true, Message: "created roaring bitmap"}
	case "ADDRB", "RBADD":
		values, err := roaringBitmapValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		added := ht.AddRoaringBitmap(key, values[0], values[1:]...)
		return CacheCommandResponse{OK: true, Message: "added roaring bitmap values", Value: strconv.Itoa(added)}
	case "REMRB", "DELRB", "RBREM", "RBDEL":
		values, err := roaringBitmapValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		removed := ht.RemoveRoaringBitmap(key, values[0], values[1:]...)
		return CacheCommandResponse{OK: true, Message: "removed roaring bitmap values", Value: strconv.Itoa(removed)}
	case "HASRB", "RBHAS", "RBEXISTS":
		values, err := roaringBitmapValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		if ht.HasRoaringBitmap(key, values[0]) {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "COUNTRB", "RBCOUNT":
		count, ok := ht.CountRoaringBitmap(key)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatUint(count, 10)}
	case "GETRB", "RBGET":
		values := ht.GetRoaringBitmap(key)
		if values == nil {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", values)
	case "INFORB", "RBINFO":
		info, ok := ht.RoaringBitmapInfo(key)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATESB", "CREATESPARSEBITSET", "SBRESERVE":
		ht.UpsertSparseBitset(key)
		return CacheCommandResponse{OK: true, Message: "created sparse bitset"}
	case "ADDSB", "SBADD":
		values, err := sparseBitsetValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		added := ht.AddSparseBitset(key, values[0], values[1:]...)
		return CacheCommandResponse{OK: true, Message: "added sparse bitset values", Value: strconv.Itoa(added)}
	case "REMSB", "DELSB", "SBREM", "SBDEL":
		values, err := sparseBitsetValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		removed := ht.RemoveSparseBitset(key, values[0], values[1:]...)
		return CacheCommandResponse{OK: true, Message: "removed sparse bitset values", Value: strconv.Itoa(removed)}
	case "HASSB", "SBHAS", "SBEXISTS":
		values, err := sparseBitsetValuesFromCommand(request)
		if err != nil {
			return commandError(err.Error())
		}
		if ht.HasSparseBitset(key, values[0]) {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "COUNTSB", "SBCOUNT":
		count, ok := ht.CountSparseBitset(key)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatUint(count, 10)}
	case "GETSB", "SBGET":
		values := ht.GetSparseBitset(key)
		if values == nil {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", values)
	case "INFOSB", "SBINFO":
		info, ok := ht.SparseBitsetInfo(key)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	case "CREATERT", "CREATERADIX", "RTCREATE":
		ht.UpsertRadixTree(key)
		return CacheCommandResponse{OK: true, Message: "created radix tree"}
	case "PUTRT", "RTPUT":
		fields, ok := commandRadixTreeFields(request)
		if !ok {
			return commandError("subkey/value or pairs is required")
		}
		added := ht.PutRadixTreeEntries(key, fields)
		return CacheCommandResponse{OK: true, Message: "stored radix tree values", Value: strconv.Itoa(added)}
	case "GETRT", "RTGET":
		subkey, ok := commandRadixTreeSubkey(request)
		if !ok {
			return commandError("subkey is required")
		}
		value, ok := ht.GetRadixTree(key, subkey)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "DELRT", "REMRT", "RTDEL", "RTREM":
		subkey, ok := commandRadixTreeSubkey(request)
		if !ok {
			return commandError("subkey is required")
		}
		if ht.DeleteRadixTree(key, subkey) {
			return CacheCommandResponse{OK: true, Message: "removed radix tree value", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "value not found", Value: "0"}
	case "HASRT", "RTEXISTS", "RTHAS":
		subkey, ok := commandRadixTreeSubkey(request)
		if !ok {
			return commandError("subkey is required")
		}
		if ht.HasRadixTree(key, subkey) {
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
	case "PREFIXRT", "SCANRT", "RTPREFIX", "RTSCAN":
		items, ok := ht.ScanRadixTree(key, strings.TrimSpace(request.Subkey))
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", items)
	case "INFORT", "RTINFO":
		info, ok := ht.RadixTreeInfo(key)
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
			estimate := ht.IncrementCountMinSketch(key, values[0], count)
			return CacheCommandResponse{OK: true, Message: "incremented count-min sketch", Value: strconv.FormatUint(estimate, 10)}
		}
		for _, value := range values {
			ht.IncrementCountMinSketch(key, value, count)
		}
		return CacheCommandResponse{OK: true, Message: "incremented count-min sketch values", Value: strconv.Itoa(len(values))}
	case "ESTCMS", "QUERYCMS", "CMSQUERY", "CMSCOUNT":
		values, ok := commandSliceValues(request)
		if !ok {
			return commandError("value is required")
		}
		estimate, ok := ht.EstimateCountMinSketch(key, values[0])
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatUint(estimate, 10)}
	case "INFOCMS", "CMSINFO":
		info, ok := ht.CountMinSketchInfo(key)
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
		estimate := ht.AddHyperLogLog(key, values[0], values[1:]...)
		return CacheCommandResponse{OK: true, Message: "added hyperloglog values", Value: strconv.FormatUint(estimate, 10)}
	case "COUNTHLL", "ESTHLL", "HLLCOUNT", "HLLCARD":
		count, ok := ht.CountHyperLogLog(key)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatUint(count, 10)}
	case "INFOHLL", "HLLINFO":
		info, ok := ht.HyperLogLogInfo(key)
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
		value := ht.GetTopK(key)
		if value == nil {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "INFOTOPK", "TOPKINFO":
		info, ok := ht.TopKInfo(key)
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
		value := ht.GetReservoirSample(key)
		if value == nil {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", value)
	case "INFORS", "RSINFO":
		info, ok := ht.ReservoirSampleInfo(key)
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
		estimate := ht.AddQuantileSketch(key, values[0], values[1:]...)
		return commandValueResponse("added quantile sketch values", estimate)
	case "ESTQ", "QUERYQ", "QQUERY", "QSQUERY", "QUANTILE":
		quantile, err := commandQuantileValue(request)
		if err != nil {
			return commandError(err.Error())
		}
		estimate, ok := ht.EstimateQuantileSketch(key, quantile)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", estimate)
	case "INFOQ", "QINFO", "INFOQS", "QSINFO":
		info, ok := ht.QuantileSketchInfo(key)
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
		update, ok := ht.AddFenwickTree(key, index, delta)
		if !ok {
			return commandError("fenwick tree update is out of range or overflows")
		}
		return commandValueResponse("updated fenwick tree", update)
	case "GETFW", "FWGET":
		index, err := commandFenwickTreeIndex(request)
		if err != nil {
			return commandError(err.Error())
		}
		value, ok := ht.GetFenwickTree(key, index)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatInt(value, 10)}
	case "SUMFW", "PREFIXFW", "FWPREFIX", "FWSUM":
		index, err := commandFenwickTreeIndex(request)
		if err != nil {
			return commandError(err.Error())
		}
		value, ok := ht.PrefixSumFenwickTree(key, index)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatInt(value, 10)}
	case "RANGEFW", "FWRANGE":
		start, end, err := commandFenwickTreeRange(request)
		if err != nil {
			return commandError(err.Error())
		}
		value, ok := ht.RangeSumFenwickTree(key, start, end)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return CacheCommandResponse{OK: true, Message: "ok", Value: strconv.FormatInt(value, 10)}
	case "INFOFW", "FWINFO":
		info, ok := ht.FenwickTreeInfo(key)
		if !ok {
			return CacheCommandResponse{OK: true, Message: "value not found"}
		}
		return commandValueResponse("ok", info)
	default:
		return commandError("unsupported command")
	}
}

func (ht *HatTrie) commandIncrementCounter(key string, by int32) (int32, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsCounter() {
		next := int64(hval.Index) + int64(by)
		if next < minCommandInt32 || next > maxCommandInt32 {
			return 0, false
		}
		hval.Index = int32(next)
	} else {
		ht.returnStorage(hval)
		ht.clearExpirationLocked(key)
		hval = HatValue{Index: by, Flags: DATAVALUE_TYPE_COUNTER}
	}
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	return hval.Index, true
}

func (ht *HatTrie) commandPutMap(key string, fields Map) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsMap() {
		ht.maps.PutEntries(hval.Index, fields)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.maps.Add(fields)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_MAP}.toValue()
	ht.recordWriteLocked(key)
}

func (ht *HatTrie) commandDumpEntry(key string) (string, bool, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.peekLocked(key)
	if hval.Empty() {
		ht.recordReadLocked(false, key)
		return "", false, nil
	}
	entry, err := ht.snapshotEntryLocked(Entry{Key: key, Value: hval})
	if err != nil {
		ht.recordReadLocked(false, key)
		return "", false, err
	}
	data, err := json.Marshal(entry)
	if err != nil {
		ht.recordReadLocked(false, key)
		return "", false, err
	}
	ht.recordReadLocked(true, key)
	return string(data), true, nil
}

func (ht *HatTrie) commandInternalSet(key string, payload string) error {
	operation, err := commandSnapshotOperation(key, payload)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	_, err = ht.applySnapshotOperationLocked(operation)
	return err
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
	return validateSnapshotEntry(entry)
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
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
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

func (ht *HatTrie) commandValueLocked(hval HatValue) (string, error) {
	switch hval.Type() {
	case DATAVALUE_TYPE_COUNTER:
		return strconv.FormatInt(int64(hval.Index), 10), nil
	case DATAVALUE_TYPE_RAW_STRING:
		return string(ht.raws.array[hval.Index]), nil
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
		data, err := json.Marshal(ht.maps.array[hval.Index])
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_SLICE:
		data, err := json.Marshal(ht.slices.array[hval.Index].Slice())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_SET:
		data, err := json.Marshal(ht.sets.array[hval.Index].Values())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_PRIORITY_QUEUE:
		data, err := json.Marshal(ht.priorityQueues.array[hval.Index].Items())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_BLOOM_FILTER:
		data, err := json.Marshal(ht.bloomFilters.array[hval.Index].Info())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_CUCKOO_FILTER:
		data, err := json.Marshal(ht.cuckooFilters.array[hval.Index].Info())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_ROARING_BITMAP:
		data, err := json.Marshal(ht.roaringBitmaps.array[hval.Index].Info())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_SPARSE_BITSET:
		data, err := json.Marshal(ht.sparseBitsets.array[hval.Index].Info())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_COUNT_MIN_SKETCH:
		data, err := json.Marshal(ht.countMinSketches.array[hval.Index].Info())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_HYPERLOGLOG:
		data, err := json.Marshal(ht.hyperLogLogs.array[hval.Index].Info())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_TOP_K:
		data, err := json.Marshal(ht.topKs.array[hval.Index].Items())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_RESERVOIR_SAMPLE:
		data, err := json.Marshal(ht.reservoirSamples.array[hval.Index].Items())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_XOR_FILTER:
		data, err := json.Marshal(ht.xorFilters.array[hval.Index].Info())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_RADIX_TREE:
		data, err := json.Marshal(ht.radixTrees.array[hval.Index].Info())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_QUANTILE_SKETCH:
		data, err := json.Marshal(ht.quantileSketches.array[hval.Index].Info())
		if err != nil {
			return "", err
		}
		return string(data), nil
	case DATAVALUE_TYPE_FENWICK_TREE:
		data, err := json.Marshal(ht.fenwickTrees.array[hval.Index].Info())
		if err != nil {
			return "", err
		}
		return string(data), nil
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
		data, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(data), nil
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
