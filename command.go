package hatriecache

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

const maxCommandTTLSeconds = int64(1<<63-1) / int64(time.Second)

type CacheCommandRequest struct {
	Command    string `json:"command"`
	Key        string `json:"key"`
	Value      string `json:"value,omitempty"`
	TTLSeconds *int64 `json:"ttl_seconds,omitempty"`
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
	case "EXISTS":
		if ht.Get(key).Empty() {
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
	case "DEL":
		if ht.Delete(key) {
			return CacheCommandResponse{OK: true, Message: "deleted"}
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
	default:
		return commandError("unsupported command")
	}
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
		ht.recordReadLocked(false)
		return "", false, nil
	}

	value, err := ht.commandValueLocked(hval)
	if err != nil {
		ht.recordReadLocked(false)
		return "", false, err
	}
	ht.recordReadLocked(true)
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
		data, err := json.Marshal(ht.slices.array[hval.Index])
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

func requirePositiveTTL(ttlSeconds *int64) (time.Duration, bool) {
	if ttlSeconds == nil || *ttlSeconds <= 0 || *ttlSeconds > maxCommandTTLSeconds {
		return 0, false
	}
	return time.Duration(*ttlSeconds) * time.Second, true
}

func commandError(message string) CacheCommandResponse {
	return CacheCommandResponse{OK: false, Message: message}
}
