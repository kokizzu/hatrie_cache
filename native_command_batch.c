#include "native_command_batch.h"

#include <limits.h>

#define HC_VALUE_INDEX_MASK ((value_t)UINT32_MAX)
#define HC_VALUE_FLAGS_SHIFT 32
#define HC_VALUE_TYPE_MASK 0x3fU
#define HC_VALUE_TYPE_COUNTER 1U

void hc_hattrie_command_batch(hattrie_t *trie, const char *keys,
                              const hc_batch_operation_t *operations,
                              hc_batch_result_t *results, size_t count) {
    size_t index;
    for (index = 0; index < count; index++) {
        const hc_batch_operation_t *operation = &operations[index];
        hc_batch_result_t *result = &results[index];
        const char *key = keys + operation->key_offset;
        value_t *location;

        result->previous = 0;
        result->value = 0;
        result->status = HC_BATCH_MISSING;

        switch (operation->operation) {
        case HC_BATCH_LOOKUP:
            location = hattrie_tryget(trie, key, operation->key_length);
            if (location != NULL) {
                result->previous = *location;
                result->value = *location;
                result->status = HC_BATCH_OK;
            }
            break;
        case HC_BATCH_SET:
            location = hattrie_get(trie, key, operation->key_length);
            result->previous = *location;
            *location = operation->input;
            result->value = *location;
            result->status = HC_BATCH_OK;
            break;
        case HC_BATCH_INCREMENT: {
            int32_t increment = (int32_t)(uint32_t)(operation->input & HC_VALUE_INDEX_MASK);
            location = hattrie_get(trie, key, operation->key_length);
            result->previous = *location;
            if (((uint32_t)(*location >> HC_VALUE_FLAGS_SHIFT) & HC_VALUE_TYPE_MASK) == HC_VALUE_TYPE_COUNTER) {
                int32_t current = (int32_t)(uint32_t)(*location & HC_VALUE_INDEX_MASK);
                int64_t next = (int64_t)current + (int64_t)increment;
                if (next < INT32_MIN || next > INT32_MAX) {
                    result->value = *location;
                    result->status = HC_BATCH_OVERFLOW;
                    break;
                }
                *location = (*location & ~HC_VALUE_INDEX_MASK) | (value_t)(uint32_t)(int32_t)next;
            } else {
                *location = ((value_t)HC_VALUE_TYPE_COUNTER << HC_VALUE_FLAGS_SHIFT) |
                            (value_t)(uint32_t)increment;
            }
            result->value = *location;
            result->status = HC_BATCH_OK;
            break;
        }
        case HC_BATCH_DELETE:
            location = hattrie_tryget(trie, key, operation->key_length);
            if (location != NULL) {
                result->previous = *location;
                result->value = *location;
                if (hattrie_del(trie, key, operation->key_length) == 0) {
                    result->status = HC_BATCH_OK;
                }
            }
            break;
        default:
            break;
        }
    }
}
