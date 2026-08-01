#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>

#include "../src/hat-trie.h"

#define LOOKUP_KEY_COUNT 1024

static volatile value_t lookup_sink;

static double monotonic_seconds(void)
{
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
        perror("clock_gettime");
        exit(2);
    }
    return (double) value.tv_sec + (double) value.tv_nsec / 1000000000.0;
}

static size_t parse_size(const char* text, size_t fallback)
{
    if (text == NULL || *text == '\0') return fallback;
    char* end = NULL;
    unsigned long long value = strtoull(text, &end, 10);
    if (end == text || *end != '\0' || value == 0 || value > (unsigned long long) ((size_t) -1)) {
        fprintf(stderr, "invalid positive size: %s\n", text);
        exit(2);
    }
    return (size_t) value;
}

static uint64_t mix_key(uint64_t value)
{
    value += UINT64_C(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

static size_t format_key(char* out, size_t out_size, size_t index, int distributed)
{
    if (distributed) {
        uint64_t words[2];
        if (out_size < sizeof(words)) {
            fprintf(stderr, "benchmark key buffer is too small\n");
            exit(2);
        }
        words[0] = mix_key((uint64_t) index);
        words[1] = (uint64_t) index;
        memcpy(out, words, sizeof(words));
        return sizeof(words);
    }
    int written = snprintf(out, out_size, "region:%012zu:shared-payload", index);
    if (written < 0 || (size_t) written >= out_size) {
        fprintf(stderr, "benchmark key overflow\n");
        exit(2);
    }
    return (size_t) written;
}

int main(int argc, char** argv)
{
    const size_t keys = parse_size(argc > 1 ? argv[1] : NULL, 100000);
    const size_t lookup_operations = parse_size(argc > 2 ? argv[2] : NULL, 10000000);
    const char* mode = argc > 3 ? argv[3] : "shared";
    int distributed = strcmp(mode, "distributed") == 0;
    if (!distributed && strcmp(mode, "shared") != 0) {
        fprintf(stderr, "invalid key mode: %s\n", mode);
        return 2;
    }
    hattrie_t* trie = hattrie_create();
    char key[96];
    size_t index;

    double start = monotonic_seconds();
    for (index = 0; index < keys; ++index) {
        size_t length = format_key(key, sizeof(key), index, distributed);
        value_t* value = hattrie_get(trie, key, length);
        if (value == NULL) {
            fprintf(stderr, "insert failed at %zu\n", index);
            return 1;
        }
        *value = (value_t) index + 1;
    }
    double insert_seconds = monotonic_seconds() - start;

    char lookup_keys[LOOKUP_KEY_COUNT][96];
    size_t lookup_lengths[LOOKUP_KEY_COUNT];
    for (index = 0; index < LOOKUP_KEY_COUNT; ++index) {
        size_t key_index = index * keys / LOOKUP_KEY_COUNT;
        lookup_lengths[index] = format_key(lookup_keys[index], sizeof(lookup_keys[index]), key_index, distributed);
        value_t* value = hattrie_get(trie, lookup_keys[index], lookup_lengths[index]);
        if (value == NULL || *value != (value_t) key_index + 1) {
            fprintf(stderr, "warm lookup failed at %zu\n", index);
            return 1;
        }
    }

    start = monotonic_seconds();
    for (index = 0; index < lookup_operations; ++index) {
        size_t lookup_index = index & (LOOKUP_KEY_COUNT - 1);
        value_t* value = hattrie_tryget(trie, lookup_keys[lookup_index], lookup_lengths[lookup_index]);
        if (value == NULL) {
            fprintf(stderr, "timed tryget failed at %zu\n", index);
            return 1;
        }
        lookup_sink ^= *value;
    }
    double tryget_seconds = monotonic_seconds() - start;

    start = monotonic_seconds();
    for (index = 0; index < lookup_operations; ++index) {
        size_t lookup_index = index & (LOOKUP_KEY_COUNT - 1);
        value_t* value = hattrie_get(trie, lookup_keys[lookup_index], lookup_lengths[lookup_index]);
        if (value == NULL) {
            fprintf(stderr, "timed get failed at %zu\n", index);
            return 1;
        }
        lookup_sink ^= *value;
    }
    double get_seconds = monotonic_seconds() - start;

    struct rusage usage;
    memset(&usage, 0, sizeof(usage));
    if (getrusage(RUSAGE_SELF, &usage) != 0) {
        perror("getrusage");
        return 2;
    }
    printf("mode=%s keys=%zu lookup_operations=%zu insert_seconds=%.9f tryget_seconds=%.9f get_seconds=%.9f size=%zu max_rss_kib=%ld\n",
           mode,
           keys,
           lookup_operations,
           insert_seconds,
           tryget_seconds,
           get_seconds,
           hattrie_size(trie),
           usage.ru_maxrss);

    hattrie_free(trie);
    return 0;
}
