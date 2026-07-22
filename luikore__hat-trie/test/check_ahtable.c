
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "str_map.h"
#include "../src/ahtable.h"
#include "../src/misc.h"

/* Simple random string generation. */
void randstr(char* x, size_t len)
{
    x[len] = '\0';
    while (len > 0) {
        x[--len] = '\x20' + (rand() % ('\x7e' - '\x20' + 1));
    }
}


const size_t n = 100000;  // how many unique strings
const size_t m_low  = 50;  // minimum length of each string
const size_t m_high = 500; // maximum length of each string
const size_t k = 200000;  // number of insertions
char** xs;

ahtable_t* T;
str_map* M;
int have_error = 0;


void setup()
{
    fprintf(stderr, "generating %zu keys ... ", n);
    xs = malloc_array_or_die(n, sizeof(char*));
    size_t i;
    size_t m;
    for (i = 0; i < n; ++i) {
        m = m_low + rand() % (m_high - m_low);
        xs[i] = malloc_or_die(m + 1);
        randstr(xs[i], m);
    }

    T = ahtable_create();
    M = str_map_create();
    fprintf(stderr, "done.\n");
}


void teardown()
{
    ahtable_free(T);
    str_map_destroy(M);

    size_t i;
    for (i = 0; i < n; ++i) {
        free(xs[i]);
    }
    free(xs);
}


void test_ahtable_insert()
{
    fprintf(stderr, "inserting %zu keys ... \n", k);

    size_t i, j;
    value_t* u;
    value_t  v;

    for (j = 0; j < k; ++j) {
        i = rand() % n;


        v = 1 + str_map_get(M, xs[i], strlen(xs[i]));
        str_map_set(M, xs[i], strlen(xs[i]), v);


        u = ahtable_get(T, xs[i], strlen(xs[i]));
        *u += 1;


        if (*u != v) {
            fprintf(stderr, "[error] tally mismatch (reported: %lu, correct: %lu)\n",
                            *u, v);
            have_error = 1;
        }
    }
    
    /* delete some keys */
    for (j = 0; j < k/100; ++j) {
        i = rand() % n;
        ahtable_del(T, xs[i], strlen(xs[i]));
        str_map_del(M, xs[i], strlen(xs[i]));
        u = ahtable_tryget(T, xs[i], strlen(xs[i]));
        if (u) {
            fprintf(stderr, "[error] deleted node found in ahtable\n");
            have_error = 1;
        }
    }

    fprintf(stderr, "done.\n");
}


void test_ahtable_iteration()
{
    fprintf(stderr, "iterating through %zu keys ... \n", k);

    ahtable_iter_t* i = ahtable_iter_begin(T, false);

    size_t count = 0;
    value_t* u;
    value_t  v;

    size_t len;
    const char* key;

    while (!ahtable_iter_finished(i)) {
        ++count;

        key = ahtable_iter_key(i, &len);
        u   = ahtable_iter_val(i);
        v   = str_map_get(M, key, len);

        if (*u != v) {
            if (v == 0) {
                fprintf(stderr, "[error] incorrect iteration (%lu, %lu)\n", *u, v);
                have_error = 1;
            }
            else {
                fprintf(stderr, "[error] incorrect iteration tally (%lu, %lu)\n", *u, v);
                have_error = 1;
            }
        }

        // this way we will see an error if the same key is iterated through
        // twice
        str_map_set(M, key, len, 0);

        ahtable_iter_next(i);
    }

    if (count != M->m) {
        fprintf(stderr, "[error] iterated through %zu element, expected %zu\n",
                count, M->m);
        have_error = 1;
    }

    ahtable_iter_free(i);

    fprintf(stderr, "done.\n");
}


int cmpkey(const char* a, size_t ka, const char* b, size_t kb)
{
    int c = memcmp(a, b, ka < kb ? ka : kb);
    return c == 0 ? (int) ka - (int) kb : c;
}


void test_ahtable_sorted_iteration()
{
    fprintf(stderr, "iterating in order through %zu keys ... \n", k);

    ahtable_iter_t* i = ahtable_iter_begin(T, true);

    size_t count = 0;
    value_t* u;
    value_t  v;

    char* prev_key = malloc_or_die(m_high + 1);
    size_t prev_len = 0;

    const char *key = NULL;
    size_t len = 0;

    while (!ahtable_iter_finished(i)) {
        memcpy(prev_key, key, len);
        prev_len = len;
        ++count;

        key = ahtable_iter_key(i, &len);
        if (prev_key != NULL && cmpkey(prev_key, prev_len, key, len) > 0) {
            fprintf(stderr, "[error] iteration is not correctly ordered.\n");
            have_error = 1;
        }

        u  = ahtable_iter_val(i);
        v  = str_map_get(M, key, len);

        if (*u != v) {
            if (v == 0) {
                fprintf(stderr, "[error] incorrect iteration (%lu, %lu)\n", *u, v);
                have_error = 1;
            }
            else {
                fprintf(stderr, "[error] incorrect iteration tally (%lu, %lu)\n", *u, v);
                have_error = 1;
            }
        }

        // this way we will see an error if the same key is iterated through
        // twice
        str_map_set(M, key, len, 0);

        ahtable_iter_next(i);
    }

    ahtable_iter_free(i);
    free(prev_key);

    fprintf(stderr, "done.\n");
}

void fill_repeated(char* x, size_t len, char c)
{
    memset(x, c, len);
    x[len] = '\0';
}


void test_ahtable_key_length_limit()
{
    fprintf(stderr, "checking key length limit... \n");

    ahtable_t* T = ahtable_create();
    char* max_key = malloc_or_die(ahtable_max_key_length + 1);
    char* too_long = malloc_or_die(ahtable_max_key_length + 2);
    fill_repeated(max_key, ahtable_max_key_length, 'm');
    fill_repeated(too_long, ahtable_max_key_length + 1, 'x');

    value_t* v = ahtable_get(T, max_key, ahtable_max_key_length);
    if (v == NULL) {
        fprintf(stderr, "[error] max length key was rejected\n");
        have_error = 1;
    }
    else {
        *v = 42;
        v = ahtable_tryget(T, max_key, ahtable_max_key_length);
        if (v == NULL || *v != 42) {
            fprintf(stderr, "[error] max length key was not retrieved\n");
            have_error = 1;
        }
    }

    v = ahtable_get(T, too_long, ahtable_max_key_length + 1);
    if (v != NULL) {
        fprintf(stderr, "[error] oversized key was inserted\n");
        have_error = 1;
    }
    if (ahtable_tryget(T, too_long, ahtable_max_key_length + 1) != NULL) {
        fprintf(stderr, "[error] oversized key was found\n");
        have_error = 1;
    }
    if (ahtable_del(T, too_long, ahtable_max_key_length + 1) == 0) {
        fprintf(stderr, "[error] oversized key was deleted\n");
        have_error = 1;
    }
    if (ahtable_size(T) != 1) {
        fprintf(stderr, "[error] oversized key changed table size to %zu\n", ahtable_size(T));
        have_error = 1;
    }

    free(max_key);
    free(too_long);
    ahtable_free(T);

    fprintf(stderr, "done.\n");
}


void test_ahtable_zero_slot_create()
{
    fprintf(stderr, "checking zero-slot create... \n");

    ahtable_t* T = ahtable_create_n(0);
    value_t* v = ahtable_get(T, "zero", 4);
    if (v == NULL) {
        fprintf(stderr, "[error] zero-slot table rejected insert\n");
        have_error = 1;
    }
    else {
        *v = 7;
        v = ahtable_tryget(T, "zero", 4);
        if (v == NULL || *v != 7) {
            fprintf(stderr, "[error] zero-slot table did not retrieve inserted value\n");
            have_error = 1;
        }
    }
    if (ahtable_size(T) != 1) {
        fprintf(stderr, "[error] zero-slot table size is %zu, expected 1\n", ahtable_size(T));
        have_error = 1;
    }
    ahtable_free(T);

    fprintf(stderr, "done.\n");
}


void test_ahtable_null_api()
{
    fprintf(stderr, "checking null table api... \n");

    if (ahtable_size(NULL) != 0) {
        fprintf(stderr, "[error] null table size was not zero\n");
        have_error = 1;
    }
    if (ahtable_get(NULL, "missing", 7) != NULL) {
        fprintf(stderr, "[error] null table get returned a value\n");
        have_error = 1;
    }
    if (ahtable_tryget(NULL, "missing", 7) != NULL) {
        fprintf(stderr, "[error] null table tryget returned a value\n");
        have_error = 1;
    }
    if (ahtable_del(NULL, "missing", 7) == 0) {
        fprintf(stderr, "[error] null table delete reported success\n");
        have_error = 1;
    }
    ahtable_t* T = ahtable_create();
    if (ahtable_get(T, NULL, 1) != NULL) {
        fprintf(stderr, "[error] invalid table key get returned a value\n");
        have_error = 1;
    }
    if (ahtable_tryget(T, NULL, 1) != NULL) {
        fprintf(stderr, "[error] invalid table key tryget returned a value\n");
        have_error = 1;
    }
    if (ahtable_del(T, NULL, 1) == 0) {
        fprintf(stderr, "[error] invalid table key delete reported success\n");
        have_error = 1;
    }
    value_t* v = ahtable_get(T, NULL, 0);
    if (v == NULL) {
        fprintf(stderr, "[error] zero-length null table key was rejected\n");
        have_error = 1;
    }
    else {
        *v = 5;
        v = ahtable_tryget(T, "", 0);
        if (v == NULL || *v != 5) {
            fprintf(stderr, "[error] zero-length null table key was not retrievable as empty key\n");
            have_error = 1;
        }
    }
    ahtable_free(T);

    ahtable_iter_t* it = ahtable_iter_begin(NULL, false);
    if (!ahtable_iter_finished(it)) {
        fprintf(stderr, "[error] null table unsorted iterator was not finished\n");
        have_error = 1;
    }
    size_t len = 123;
    if (ahtable_iter_key(it, &len) != NULL || len != 0) {
        fprintf(stderr, "[error] null table unsorted iterator returned a key\n");
        have_error = 1;
    }
    if (ahtable_iter_val(it) != NULL) {
        fprintf(stderr, "[error] null table unsorted iterator returned a value\n");
        have_error = 1;
    }
    ahtable_iter_next(it);
    if (!ahtable_iter_finished(it)) {
        fprintf(stderr, "[error] null table unsorted iterator advanced to a value\n");
        have_error = 1;
    }
    ahtable_iter_free(it);

    it = ahtable_iter_begin(NULL, true);
    if (!ahtable_iter_finished(it)) {
        fprintf(stderr, "[error] null table sorted iterator was not finished\n");
        have_error = 1;
    }
    len = 123;
    if (ahtable_iter_key(it, &len) != NULL || len != 0) {
        fprintf(stderr, "[error] null table sorted iterator returned a key\n");
        have_error = 1;
    }
    if (ahtable_iter_val(it) != NULL) {
        fprintf(stderr, "[error] null table sorted iterator returned a value\n");
        have_error = 1;
    }
    ahtable_iter_free(it);

    ahtable_iter_next(NULL);
    if (!ahtable_iter_finished(NULL)) {
        fprintf(stderr, "[error] null iterator was not finished\n");
        have_error = 1;
    }
    len = 123;
    if (ahtable_iter_key(NULL, &len) != NULL || len != 0) {
        fprintf(stderr, "[error] null iterator returned a key\n");
        have_error = 1;
    }
    if (ahtable_iter_key(NULL, NULL) != NULL) {
        fprintf(stderr, "[error] null iterator returned a key without length output\n");
        have_error = 1;
    }
    if (ahtable_iter_val(NULL) != NULL) {
        fprintf(stderr, "[error] null iterator returned a value\n");
        have_error = 1;
    }
    ahtable_iter_free(NULL);
    ahtable_clear(NULL);
    ahtable_free(NULL);

    fprintf(stderr, "done.\n");
}


void test_ahtable_empty_slot_operations()
{
    fprintf(stderr, "checking empty slot operations... \n");

    ahtable_t* T = ahtable_create_n(1);
    if (ahtable_tryget(T, "missing", 7) != NULL) {
        fprintf(stderr, "[error] missing key was found in empty table\n");
        have_error = 1;
    }
    if (ahtable_del(T, "missing", 7) == 0) {
        fprintf(stderr, "[error] missing key was deleted from empty table\n");
        have_error = 1;
    }

    ahtable_iter_t* it = ahtable_iter_begin(T, false);
    if (!ahtable_iter_finished(it)) {
        fprintf(stderr, "[error] unsorted iterator over empty table was not finished\n");
        have_error = 1;
    }
    ahtable_iter_free(it);

    it = ahtable_iter_begin(T, true);
    if (!ahtable_iter_finished(it)) {
        fprintf(stderr, "[error] sorted iterator over empty table was not finished\n");
        have_error = 1;
    }
    ahtable_iter_free(it);

    ahtable_free(T);
    fprintf(stderr, "done.\n");
}


void test_ahtable_sorted_binary_boundaries()
{
    fprintf(stderr, "checking sorted binary key boundaries... \n");

    unsigned char zero[] = {0x00};
    unsigned char zero_zero[] = {0x00, 0x00};
    unsigned char zero_high[] = {0x00, 0xff};
    unsigned char ascii[] = {'a'};
    unsigned char ascii_zero[] = {'a', 0x00};
    unsigned char ascii_next[] = {'a', 'a'};
    unsigned char high[] = {0x80};
    unsigned char highest[] = {0xff};
    unsigned char length127[127];
    unsigned char length128[128];
    unsigned char length129[129];
    memset(length127, 'z', sizeof(length127));
    memset(length128, 'z', sizeof(length128));
    memset(length129, 'z', sizeof(length129));

    struct binary_key {
        const unsigned char* data;
        size_t len;
    } keys[] = {
        {length128, sizeof(length128)},
        {highest, sizeof(highest)},
        {zero_high, sizeof(zero_high)},
        {ascii_next, sizeof(ascii_next)},
        {length127, sizeof(length127)},
        {zero, sizeof(zero)},
        {high, sizeof(high)},
        {ascii_zero, sizeof(ascii_zero)},
        {length129, sizeof(length129)},
        {zero_zero, sizeof(zero_zero)},
        {ascii, sizeof(ascii)},
    };
    const size_t key_count = sizeof(keys) / sizeof(keys[0]);
    ahtable_t* table = ahtable_create_n(1);
    size_t index;
    for (index = 0; index < key_count; ++index) {
        value_t* value = ahtable_get(table, (const char*) keys[index].data, keys[index].len);
        if (value == NULL) {
            fprintf(stderr, "[error] binary boundary insert %zu failed\n", index);
            have_error = 1;
            continue;
        }
        *value = index + 1;
    }

    ahtable_iter_t* iterator = ahtable_iter_begin(table, true);
    const char* previous = NULL;
    size_t previous_len = 0;
    size_t count = 0;
    while (!ahtable_iter_finished(iterator)) {
        size_t len = 0;
        const char* key = ahtable_iter_key(iterator, &len);
        if (previous != NULL && cmpkey(previous, previous_len, key, len) >= 0) {
            fprintf(stderr, "[error] binary boundary iteration is not strictly ordered at %zu\n", count);
            have_error = 1;
        }
        previous = key;
        previous_len = len;
        ++count;
        ahtable_iter_next(iterator);
    }
    if (count != key_count) {
        fprintf(stderr, "[error] binary boundary iteration count %zu, expected %zu\n", count, key_count);
        have_error = 1;
    }
    ahtable_iter_free(iterator);
    ahtable_free(table);
    fprintf(stderr, "done.\n");
}


void test_ahtable_sorted_long_prefix()
{
    fprintf(stderr, "checking sorted long shared prefix... \n");

    const size_t key_count = 64;
    const size_t key_len = 1024;
    unsigned char key[1024];
    memset(key, 'p', sizeof(key));
    ahtable_t* table = ahtable_create_n(1);
    size_t index;
    for (index = key_count; index > 0; --index) {
        key[key_len - 1] = (unsigned char) (index - 1);
        value_t* value = ahtable_get(table, (const char*) key, key_len);
        if (value == NULL) {
            fprintf(stderr, "[error] long-prefix insert %zu failed\n", index - 1);
            have_error = 1;
            continue;
        }
        *value = index;
    }

    ahtable_iter_t* iterator = ahtable_iter_begin(table, true);
    const char* previous = NULL;
    size_t previous_len = 0;
    size_t count = 0;
    while (!ahtable_iter_finished(iterator)) {
        size_t len = 0;
        const char* current = ahtable_iter_key(iterator, &len);
        if (previous != NULL && cmpkey(previous, previous_len, current, len) >= 0) {
            fprintf(stderr, "[error] long-prefix iteration is not strictly ordered at %zu\n", count);
            have_error = 1;
        }
        previous = current;
        previous_len = len;
        ++count;
        ahtable_iter_next(iterator);
    }
    if (count != key_count) {
        fprintf(stderr, "[error] long-prefix iteration count %zu, expected %zu\n", count, key_count);
        have_error = 1;
    }
    ahtable_iter_free(iterator);
    ahtable_free(table);
    fprintf(stderr, "done.\n");
}


void test_ahtable_delete_releases_empty_slot()
{
    fprintf(stderr, "checking delete empty slot release... \n");

    ahtable_t* T = ahtable_create_n(1);
    value_t* v = ahtable_get(T, "alpha", 5);
    if (v == NULL) {
        fprintf(stderr, "[error] delete release test insert failed\n");
        have_error = 1;
    }
    else {
        *v = 17;
    }
    if (T->slots[0] == NULL || T->slot_sizes[0] == 0) {
        fprintf(stderr, "[error] inserted key did not allocate slot storage\n");
        have_error = 1;
    }
    if (ahtable_del(T, "alpha", 5) != 0) {
        fprintf(stderr, "[error] delete release test delete failed\n");
        have_error = 1;
    }
    if (ahtable_size(T) != 0) {
        fprintf(stderr, "[error] table size after final delete is %zu, expected 0\n", ahtable_size(T));
        have_error = 1;
    }
    if (T->slots[0] != NULL || T->slot_sizes[0] != 0) {
        fprintf(stderr, "[error] final delete retained empty slot storage\n");
        have_error = 1;
    }
    if (ahtable_tryget(T, "alpha", 5) != NULL) {
        fprintf(stderr, "[error] deleted key was still found after empty slot release\n");
        have_error = 1;
    }
    v = ahtable_get(T, "beta", 4);
    if (v == NULL) {
        fprintf(stderr, "[error] insert after empty slot release failed\n");
        have_error = 1;
    }
    else {
        *v = 23;
    }

    ahtable_free(T);
    fprintf(stderr, "done.\n");
}


void test_ahtable_delete_compacts_nonempty_slot()
{
    fprintf(stderr, "checking delete nonempty slot compaction... \n");

    ahtable_t* T = ahtable_create_n(1);
    value_t* v = ahtable_get(T, "alpha", 5);
    if (v == NULL) {
        fprintf(stderr, "[error] nonempty compaction alpha insert failed\n");
        have_error = 1;
    }
    else {
        *v = 11;
    }
    v = ahtable_get(T, "beta", 4);
    if (v == NULL) {
        fprintf(stderr, "[error] nonempty compaction beta insert failed\n");
        have_error = 1;
    }
    else {
        *v = 22;
    }
    v = ahtable_get(T, "gamma", 5);
    if (v == NULL) {
        fprintf(stderr, "[error] nonempty compaction gamma insert failed\n");
        have_error = 1;
    }
    else {
        *v = 33;
    }

    if (ahtable_del(T, "beta", 4) != 0) {
        fprintf(stderr, "[error] nonempty compaction delete failed\n");
        have_error = 1;
    }
    if (ahtable_size(T) != 2) {
        fprintf(stderr, "[error] table size after middle delete is %zu, expected 2\n", ahtable_size(T));
        have_error = 1;
    }
    if (ahtable_tryget(T, "beta", 4) != NULL) {
        fprintf(stderr, "[error] deleted middle key was still found\n");
        have_error = 1;
    }
    v = ahtable_tryget(T, "alpha", 5);
    if (v == NULL || *v != 11) {
        fprintf(stderr, "[error] alpha missing after nonempty slot compaction\n");
        have_error = 1;
    }
    v = ahtable_tryget(T, "gamma", 5);
    if (v == NULL || *v != 33) {
        fprintf(stderr, "[error] gamma missing after nonempty slot compaction\n");
        have_error = 1;
    }

    ahtable_iter_t* it = ahtable_iter_begin(T, false);
    size_t count = 0;
    while (!ahtable_iter_finished(it)) {
        count++;
        ahtable_iter_next(it);
    }
    ahtable_iter_free(it);
    if (count != 2) {
        fprintf(stderr, "[error] unsorted iteration after nonempty compaction returned %zu entries, expected 2\n", count);
        have_error = 1;
    }

    v = ahtable_get(T, "delta", 5);
    if (v == NULL) {
        fprintf(stderr, "[error] insert after nonempty slot compaction failed\n");
        have_error = 1;
    }
    else {
        *v = 44;
    }

    ahtable_free(T);
    fprintf(stderr, "done.\n");
}


void test_ahtable_clear_resets_size()
{
    fprintf(stderr, "checking clear reset... \n");

    ahtable_t* T = ahtable_create_n(2);
    value_t* v = ahtable_get(T, "alpha", 5);
    if (v == NULL) {
        fprintf(stderr, "[error] clear test insert failed\n");
        have_error = 1;
    }
    else {
        *v = 11;
    }
    if (ahtable_size(T) != 1) {
        fprintf(stderr, "[error] table size before clear is %zu, expected 1\n", ahtable_size(T));
        have_error = 1;
    }

    ahtable_clear(T);
    if (ahtable_size(T) != 0) {
        fprintf(stderr, "[error] table size after clear is %zu, expected 0\n", ahtable_size(T));
        have_error = 1;
    }
    if (ahtable_tryget(T, "alpha", 5) != NULL) {
        fprintf(stderr, "[error] cleared key was still found\n");
        have_error = 1;
    }
    v = ahtable_get(T, "beta", 4);
    if (v == NULL) {
        fprintf(stderr, "[error] clear test insert after clear failed\n");
        have_error = 1;
    }
    else {
        *v = 22;
        v = ahtable_tryget(T, "beta", 4);
        if (v == NULL || *v != 22) {
            fprintf(stderr, "[error] clear test could not retrieve post-clear value\n");
            have_error = 1;
        }
    }

    ahtable_free(T);
    fprintf(stderr, "done.\n");
}

void test_ahtable_amortized_slot_growth()
{
    fprintf(stderr, "checking amortized slot growth... \n");

    const size_t keys = 63;
    ahtable_t* T = ahtable_create_n(1);
    size_t i;
    char key[32];
    for (i = 0; i < keys; ++i) {
        int len = snprintf(key, sizeof(key), "slot-growth-%04zu", i);
        value_t* value = ahtable_get(T, key, (size_t) len);
        if (value == NULL) {
            fprintf(stderr, "[error] amortized growth insert %zu failed\n", i);
            have_error = 1;
            break;
        }
        *value = i + 1;
    }

    size_t used = ahtable_slot_used_bytes(T);
    size_t capacity = ahtable_slot_capacity_bytes(T);
    size_t reallocations = ahtable_slot_reallocations(T);
    if (capacity <= used) {
        fprintf(stderr, "[error] slot capacity %zu did not retain bounded growth room above %zu used bytes\n", capacity, used);
        have_error = 1;
    }
    if (capacity > used + used / 3 + 64) {
        fprintf(stderr, "[error] slot capacity %zu exceeds bounded growth room for %zu used bytes\n", capacity, used);
        have_error = 1;
    }
    if (reallocations >= keys * 3 / 4) {
        fprintf(stderr, "[error] slot used %zu reallocations for %zu inserts, expected amortized growth\n", reallocations, keys);
        have_error = 1;
    }

    for (i = 0; i < keys - 5; ++i) {
        int len = snprintf(key, sizeof(key), "slot-growth-%04zu", i);
        if (ahtable_del(T, key, (size_t) len) != 0) {
            fprintf(stderr, "[error] amortized growth delete %zu failed\n", i);
            have_error = 1;
        }
    }
    used = ahtable_slot_used_bytes(T);
    capacity = ahtable_slot_capacity_bytes(T);
    if (capacity > used + used / 3 + 64) {
        fprintf(stderr, "[error] sparse slot capacity %zu did not shrink near %zu used bytes\n", capacity, used);
        have_error = 1;
    }
    for (; i < keys; ++i) {
        int len = snprintf(key, sizeof(key), "slot-growth-%04zu", i);
        if (ahtable_del(T, key, (size_t) len) != 0) {
            fprintf(stderr, "[error] amortized growth tail delete %zu failed\n", i);
            have_error = 1;
        }
    }
    if (ahtable_slot_used_bytes(T) != 0 || ahtable_slot_capacity_bytes(T) != 0) {
        fprintf(stderr, "[error] empty slot retained used/capacity bytes after delete\n");
        have_error = 1;
    }
    ahtable_free(T);
    fprintf(stderr, "done.\n");
}


void test_checked_array_size()
{
    fprintf(stderr, "checking allocation size guards... \n");

    size_t bytes = 0;
    if (checked_array_size(3, sizeof(char*), &bytes) != 0 ||
        bytes != 3 * sizeof(char*)) {
        fprintf(stderr, "[error] checked_array_size rejected valid multiplication\n");
        have_error = 1;
    }

    if (checked_array_size(((size_t) -1) / 2 + 1, 2, &bytes) == 0) {
        fprintf(stderr, "[error] checked_array_size accepted overflowing multiplication\n");
        have_error = 1;
    }

    if (checked_array_size(0, (size_t) -1, &bytes) != 0 || bytes != 0) {
        fprintf(stderr, "[error] checked_array_size rejected zero-sized allocation\n");
        have_error = 1;
    }

    fprintf(stderr, "done.\n");
}


int main()
{
    test_checked_array_size();
    test_ahtable_zero_slot_create();
    test_ahtable_null_api();
    test_ahtable_empty_slot_operations();
    test_ahtable_sorted_binary_boundaries();
    test_ahtable_sorted_long_prefix();
    test_ahtable_delete_releases_empty_slot();
    test_ahtable_delete_compacts_nonempty_slot();
    test_ahtable_clear_resets_size();
    test_ahtable_amortized_slot_growth();
    test_ahtable_key_length_limit();

    setup();
    test_ahtable_insert();
    test_ahtable_iteration();
    teardown();

    setup();
    test_ahtable_insert();
    test_ahtable_sorted_iteration();
    teardown();

    if (have_error) {
        return -1;
    }
    return 0;
}
