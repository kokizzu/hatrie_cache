package hatriecache

import (
	"encoding/json"
	"errors"
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
		if response, ok := validateOptionalCommandTTL(request.TTLSeconds); ok && !response.OK {
			return response
		}
		ht.UpsertString(key, request.Value)
		if response, ok := ht.applyCommandTTL(key, request.TTLSeconds); ok {
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
		if response, ok := validateOptionalCommandTTL(request.TTLSeconds); ok && !response.OK {
			return response
		}
		ht.UpsertCounter(key, value)
		if response, ok := ht.applyCommandTTL(key, request.TTLSeconds); ok {
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
		current := ht.maps.array[hval.Index]
		if current == nil {
			current = Map{}
		}
		for subkey, value := range fields {
			current[subkey] = value
		}
		ht.maps.Put(hval.Index, current)
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
	entry, err := decodeSnapshotEntryJSON([]byte(payload))
	if err != nil {
		return snapshotOperation{}, err
	}
	if strings.TrimSpace(entry.Key) == "" {
		entry.Key = key
	} else if entry.Key != key {
		return snapshotOperation{}, errors.New("snapshot entry key does not match request key")
	}
	return validateSnapshotEntry(entry)
}

func (ht *HatTrie) applyCommandTTL(key string, ttlSeconds *int64) (CacheCommandResponse, bool) {
	if ttlSeconds == nil {
		return CacheCommandResponse{}, false
	}
	ttl, ok := requirePositiveTTL(ttlSeconds)
	if !ok {
		return commandError("ttl_seconds must be positive"), true
	}
	if !ht.Expire(key, ttl) {
		return commandError("failed to set ttl"), true
	}
	return CacheCommandResponse{OK: true, Message: "stored with ttl"}, true
}

func validateOptionalCommandTTL(ttlSeconds *int64) (CacheCommandResponse, bool) {
	if ttlSeconds == nil {
		return CacheCommandResponse{}, false
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
