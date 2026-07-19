#ifndef HATRIE_CACHE_NATIVE_COMMAND_BATCH_H
#define HATRIE_CACHE_NATIVE_COMMAND_BATCH_H

#include <stddef.h>
#include <stdint.h>

#include "luikore__hat-trie/src/hat-trie.h"

enum {
    HC_BATCH_LOOKUP = 1,
    HC_BATCH_SET = 2,
    HC_BATCH_INCREMENT = 3,
    HC_BATCH_DELETE = 4
};

enum {
    HC_BATCH_MISSING = 0,
    HC_BATCH_OK = 1,
    HC_BATCH_OVERFLOW = 2
};

typedef struct {
    uint32_t key_offset;
    uint32_t key_length;
    uint8_t operation;
    uint8_t reserved[7];
    value_t input;
} hc_batch_operation_t;

typedef struct {
    value_t previous;
    value_t value;
    uint8_t status;
    uint8_t reserved[7];
} hc_batch_result_t;

void hc_hattrie_command_batch(hattrie_t *trie, const char *keys,
                              const hc_batch_operation_t *operations,
                              hc_batch_result_t *results, size_t count);

#endif
