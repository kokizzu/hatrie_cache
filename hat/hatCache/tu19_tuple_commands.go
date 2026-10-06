package hatCache

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"hatrie_cache/hat/hatDataStructure"
)

const maxTupleCommandUpdates = 4096

var errTupleCommandMissingPayload = errors.New("tuple payload is required")

func executeTupleSetCommand(ht *HatTrie, key string, request CacheCommandRequest) CacheCommandResponse {
	if request.TTLSeconds != nil || request.UnixSeconds != nil {
		return commandError("tuple commands do not accept expiration options")
	}
	payload, err := tupleCommandPayload(request)
	if err != nil {
		return commandError(err.Error())
	}
	if err := ht.UpsertBytesChecked(key, payload); err != nil {
		return commandError(err.Error())
	}
	return CacheCommandResponse{OK: true, Message: "stored versioned tuple"}
}

func executeTupleGetCommand(ht *HatTrie, key string) CacheCommandResponse {
	payload, err := ht.GetBytesChecked(key)
	if err != nil {
		return commandError(err.Error())
	}
	if len(payload) == 0 {
		return CacheCommandResponse{OK: true, Message: "key not found"}
	}
	if _, err := hatDataStructure.UnmarshalVersionedTuple(payload); err != nil {
		return commandError("stored value is not a versioned tuple: " + err.Error())
	}
	return CacheCommandResponse{
		OK:      true,
		Message: "ok",
		Value:   base64.StdEncoding.EncodeToString(payload),
	}
}

func executeTupleUpdateCommand(ht *HatTrie, key string, request CacheCommandRequest) CacheCommandResponse {
	if request.TTLSeconds != nil || request.UnixSeconds != nil {
		return commandError("tuple commands do not accept expiration options")
	}
	updates, err := tupleCommandFieldUpdates(request.Values)
	if err != nil {
		return commandError(err.Error())
	}
	found, err := ht.applyTupleFieldUpdatesChecked(key, updates)
	if err != nil {
		return commandError(err.Error())
	}
	if !found {
		return CacheCommandResponse{OK: true, Message: "key not found"}
	}
	return CacheCommandResponse{OK: true, Message: "updated versioned tuple"}
}

func tupleCommandPayload(request CacheCommandRequest) ([]byte, error) {
	payload := request.BinaryValue
	if len(payload) == 0 {
		encoded := strings.TrimSpace(request.Value)
		if encoded == "" {
			return nil, errTupleCommandMissingPayload
		}
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("tuple payload must be standard base64: %w", err)
		}
		payload = decoded
	}
	if len(payload) > hatDataStructure.MaxVersionedTupleWireBytes {
		return nil, fmt.Errorf("tuple payload exceeds %d bytes", hatDataStructure.MaxVersionedTupleWireBytes)
	}
	if _, err := hatDataStructure.UnmarshalVersionedTuple(payload); err != nil {
		return nil, fmt.Errorf("invalid versioned tuple payload: %w", err)
	}
	return append([]byte(nil), payload...), nil
}

func tupleCommandFieldUpdates(values []any) ([]hatDataStructure.TupleFieldUpdate, error) {
	if len(values) == 0 {
		return nil, errors.New("tuple update values are required")
	}
	if len(values) > maxTupleCommandUpdates {
		return nil, fmt.Errorf("tuple update has %d operations; maximum is %d", len(values), maxTupleCommandUpdates)
	}
	updates := make([]hatDataStructure.TupleFieldUpdate, len(values))
	seen := make(map[int]struct{}, len(values))
	for operationIndex, raw := range values {
		operation, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("tuple update operation %d must be an object", operationIndex)
		}
		index, err := tupleCommandNonNegativeInt(operation["index"], "index")
		if err != nil {
			return nil, fmt.Errorf("tuple update operation %d: %w", operationIndex, err)
		}
		if _, exists := seen[index]; exists {
			return nil, fmt.Errorf("tuple update field %d appears more than once", index)
		}
		seen[index] = struct{}{}
		kind, ok := operation["kind"].(string)
		if !ok || strings.TrimSpace(kind) == "" {
			return nil, fmt.Errorf("tuple update operation %d: kind is required", operationIndex)
		}
		update := hatDataStructure.TupleFieldUpdate{Index: index}
		switch strings.ToLower(strings.TrimSpace(kind)) {
		case "set":
			update.Kind = hatDataStructure.TupleFieldSet
			update.Value, err = tupleCommandBytes(operation["value"], "value")
		case "splice":
			update.Kind = hatDataStructure.TupleFieldSplice
			update.Start, err = tupleCommandNonNegativeInt(operation["start"], "start")
			if err == nil {
				update.Remove, err = tupleCommandNonNegativeInt(operation["remove"], "remove")
			}
			if err == nil {
				update.Insert, err = tupleCommandBytes(operation["insert"], "insert")
			}
		case "add_int64":
			update.Kind = hatDataStructure.TupleFieldAddInt64
			update.Delta, err = tupleCommandInt64(operation["delta"], "delta")
		default:
			return nil, fmt.Errorf("tuple update operation %d: unsupported kind %q", operationIndex, kind)
		}
		if err != nil {
			return nil, fmt.Errorf("tuple update operation %d: %w", operationIndex, err)
		}
		updates[operationIndex] = update
	}
	return updates, nil
}

func tupleCommandBytes(value any, name string) ([]byte, error) {
	var decoded []byte
	switch typed := value.(type) {
	case string:
		var err error
		decoded, err = base64.StdEncoding.DecodeString(typed)
		if err != nil {
			return nil, fmt.Errorf("%s must be standard base64: %w", name, err)
		}
	case []byte:
		decoded = append([]byte(nil), typed...)
	default:
		return nil, fmt.Errorf("%s must be standard base64", name)
	}
	if len(decoded) > hatDataStructure.MaxVersionedTupleWireBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", name, hatDataStructure.MaxVersionedTupleWireBytes)
	}
	return decoded, nil
}

func tupleCommandNonNegativeInt(value any, name string) (int, error) {
	parsed, err := tupleCommandInt64(value, name)
	if err != nil {
		return 0, err
	}
	if parsed < 0 || parsed > int64(int(^uint(0)>>1)) {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return int(parsed), nil
}

func tupleCommandInt64(value any, name string) (int64, error) {
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, fmt.Errorf("%s is outside int64 range", name)
		}
		return int64(typed), nil
	case uint8:
		return int64(typed), nil
	case uint16:
		return int64(typed), nil
	case uint32:
		return int64(typed), nil
	case uint64:
		if typed > math.MaxInt64 {
			return 0, fmt.Errorf("%s is outside int64 range", name)
		}
		return int64(typed), nil
	case float32:
		value := float64(typed)
		if math.Trunc(value) != value || value < math.MinInt64 || value > math.MaxInt64 {
			return 0, fmt.Errorf("%s must be an integer", name)
		}
		return int64(value), nil
	case float64:
		if math.Trunc(typed) != typed || typed < math.MinInt64 || typed > math.MaxInt64 {
			return 0, fmt.Errorf("%s must be an int64", name)
		}
		return int64(typed), nil
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, fmt.Errorf("%s must be an int64", name)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("%s must be an integer", name)
	}
}

func (ht *HatTrie) applyTupleFieldUpdatesChecked(key string, updates []hatDataStructure.TupleFieldUpdate) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.applyTupleFieldUpdatesChecked(key, updates)
	}
	if err := validateKey(key); err != nil {
		return false, err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		return false, nil
	}
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return false, nil
	}
	if !hval.IsBytesAtRaws() {
		return false, errors.New("tuple key does not hold a byte value")
	}
	var payload []byte
	var err error
	if hval.OnDisk() {
		payload, err = ht.disks.Get(hval.Index)
	} else {
		payload = cloneBytes(ht.raws.array[hval.Index])
	}
	if err != nil {
		return false, err
	}
	tuple, err := hatDataStructure.UnmarshalVersionedTuple(payload)
	if err != nil {
		return false, fmt.Errorf("stored value is not a versioned tuple: %w", err)
	}
	updated, err := tuple.ApplyFieldUpdates(updates)
	if err != nil {
		return false, err
	}
	updatedPayload, err := hatDataStructure.MarshalVersionedTuple(updated)
	if err != nil {
		return false, err
	}
	next, err := ht.storeBytesValueLocked(hval, updatedPayload)
	if err != nil {
		return false, err
	}
	ht.clearExpirationLocked(key)
	*rawPtr = next.toValue()
	ht.recordWriteLocked(key)
	return true, nil
}
