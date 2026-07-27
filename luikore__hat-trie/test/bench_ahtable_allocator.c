#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>

#include "../src/ahtable.h"

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

static size_t format_key(char* out, size_t out_size, const char* prefix, size_t index)
{
    int written = snprintf(out, out_size, "%s:%012zu:payload", prefix, index);
    if (written < 0 || (size_t) written >= out_size) {
        fprintf(stderr, "benchmark key overflow\n");
        exit(2);
    }
    return (size_t) written;
}

int main(int argc, char** argv)
{
    const size_t keys = parse_size(argc > 1 ? argv[1] : NULL, 100000);
    const size_t slots = parse_size(argc > 2 ? argv[2] : NULL, 4096);
    const size_t lookup_operations = parse_size(argc > 3 ? argv[3] : NULL, 10000000);
    ahtable_t* table = ahtable_create_n(slots);
    char key[96];
    size_t i;

    double start = monotonic_seconds();
    for (i = 0; i < keys; ++i) {
        size_t len = format_key(key, sizeof(key), "primary", i);
        value_t* value = ahtable_get(table, key, len);
        if (value == NULL) {
            fprintf(stderr, "insert failed at %zu\n", i);
            return 1;
        }
        *value = i + 1;
    }
    double insert_seconds = monotonic_seconds() - start;
    size_t insert_reallocations = ahtable_slot_reallocations(table);

    start = monotonic_seconds();
    for (i = 0; i < keys; i += 2) {
        size_t len = format_key(key, sizeof(key), "primary", i);
        if (ahtable_del(table, key, len) != 0) {
            fprintf(stderr, "delete failed at %zu\n", i);
            return 1;
        }
    }
    for (i = 0; i < keys; i += 2) {
        size_t len = format_key(key, sizeof(key), "replacement", i);
        value_t* value = ahtable_get(table, key, len);
        if (value == NULL) {
            fprintf(stderr, "replacement insert failed at %zu\n", i);
            return 1;
        }
        *value = i + 2;
    }
    double churn_seconds = monotonic_seconds() - start;

    if (ahtable_size(table) != keys) {
        fprintf(stderr, "table size %zu, expected %zu\n", ahtable_size(table), keys);
        return 1;
    }
    for (i = 0; i < keys; ++i) {
        const char* prefix = (i % 2 == 0) ? "replacement" : "primary";
        size_t len = format_key(key, sizeof(key), prefix, i);
        value_t* value = ahtable_tryget(table, key, len);
        value_t expected = (i % 2 == 0) ? i + 2 : i + 1;
        if (value == NULL || *value != expected) {
            fprintf(stderr, "verification failed at %zu\n", i);
            return 1;
        }
    }

    char lookup_keys[LOOKUP_KEY_COUNT][96];
    size_t lookup_lengths[LOOKUP_KEY_COUNT];
    for (i = 0; i < LOOKUP_KEY_COUNT; ++i) {
        size_t key_index = i * keys / LOOKUP_KEY_COUNT;
        const char* prefix = (key_index % 2 == 0) ? "replacement" : "primary";
        lookup_lengths[i] = format_key(lookup_keys[i], sizeof(lookup_keys[i]), prefix, key_index);
    }
    start = monotonic_seconds();
    for (i = 0; i < lookup_operations; ++i) {
        size_t lookup_index = i & (LOOKUP_KEY_COUNT - 1);
        value_t* value = ahtable_tryget(table, lookup_keys[lookup_index], lookup_lengths[lookup_index]);
        if (value == NULL) {
            fprintf(stderr, "timed lookup failed at %zu\n", i);
            return 1;
        }
        lookup_sink ^= *value;
    }
    double lookup_seconds = monotonic_seconds() - start;

    struct rusage usage;
    memset(&usage, 0, sizeof(usage));
    if (getrusage(RUSAGE_SELF, &usage) != 0) {
        perror("getrusage");
        return 2;
    }
    printf("keys=%zu slots=%zu lookup_operations=%zu insert_seconds=%.9f churn_seconds=%.9f lookup_seconds=%.9f used_bytes=%zu capacity_bytes=%zu insert_reallocations=%zu total_reallocations=%zu max_rss_kib=%ld\n",
           keys,
           slots,
           lookup_operations,
           insert_seconds,
           churn_seconds,
           lookup_seconds,
           ahtable_slot_used_bytes(table),
           ahtable_slot_capacity_bytes(table),
           insert_reallocations,
           ahtable_slot_reallocations(table),
           usage.ru_maxrss);

    ahtable_free(table);
    return 0;
}
