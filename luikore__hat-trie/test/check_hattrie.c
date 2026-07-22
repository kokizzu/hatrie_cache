
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "str_map.h"
#include "../src/ahtable.h"
#include "../src/hat-trie.h"
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
const size_t d = 50000;

char** xs;
char** ds;

hattrie_t* T;
str_map* M;
int have_error = 0;

void setup()
{
    fprintf(stderr, "generating %zu keys ... ", n);
    xs = malloc_array_or_die(n, sizeof(char*));
    ds = malloc_array_or_die(d, sizeof(char*));
    size_t i;
    size_t m;
    for (i = 0; i < n; ++i) {
        m = m_low + rand() % (m_high - m_low);
        xs[i] = malloc_or_die(m + 1);
        randstr(xs[i], m);
    }
    for (i = 0; i < d; ++i) {
        m = rand()%n;
        ds[i] = xs[m];
    }

    T = hattrie_create();
    M = str_map_create();
    fprintf(stderr, "done.\n");
}


void teardown()
{
    hattrie_free(T);
    str_map_destroy(M);

    size_t i;
    for (i = 0; i < n; ++i) {
        free(xs[i]);
    }
    free(xs);
    free(ds);
}


void test_hattrie_insert()
{
    fprintf(stderr, "inserting %zu keys ... \n", k);

    size_t i, j;
    value_t* u;
    value_t  v;

    for (j = 0; j < k; ++j) {
        i = rand() % n;


        v = 1 + str_map_get(M, xs[i], strlen(xs[i]));
        str_map_set(M, xs[i], strlen(xs[i]), v);


        u = hattrie_get(T, xs[i], strlen(xs[i]));
        *u += 1;


        if (*u != v) {
            fprintf(stderr, "[error] tally mismatch (reported: %lu, correct: %lu)\n",
                            *u, v);
            have_error = 1;
        }
    }

    fprintf(stderr, "deleting %zu keys ... \n", d);
    for (j = 0; j < d; ++j) {
        str_map_del(M, ds[j], strlen(ds[j]));
        hattrie_del(T, ds[j], strlen(ds[j]));
        u = hattrie_tryget(T, ds[j], strlen(ds[j]));
        if (u) {
            fprintf(stderr, "[error] item %zu still found in trie after delete\n",
                    j);
            have_error = 1;
        }
    }

    fprintf(stderr, "done.\n");
}



void test_hattrie_iteration()
{
    fprintf(stderr, "iterating through %zu keys ... \n", k);

    hattrie_iter_t* i = hattrie_iter_begin(T, false);

    size_t count = 0;
    value_t* u;
    value_t  v;

    size_t len;
    const char* key;

    while (!hattrie_iter_finished(i)) {
        ++count;

        key = hattrie_iter_key(i, &len);
        u   = hattrie_iter_val(i);

        v = str_map_get(M, key, len);

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

        hattrie_iter_next(i);
    }

    if (count != M->m) {
        fprintf(stderr, "[error] iterated through %zu element, expected %zu\n",
                count, M->m);
        have_error = 1;
    }

    hattrie_iter_free(i);

    fprintf(stderr, "done.\n");
}


int cmpkey(const char* a, size_t ka, const char* b, size_t kb)
{
    int c = memcmp(a, b, ka < kb ? ka : kb);
    return c == 0 ? (int) ka - (int) kb : c;
}


void test_hattrie_sorted_iteration()
{
    fprintf(stderr, "iterating in order through %zu keys ... \n", k);

    hattrie_iter_t* i = hattrie_iter_begin(T, true);

    size_t count = 0;
    value_t* u;
    value_t  v;

    char* key_copy = malloc_or_die(m_high + 1);
    char* prev_key = malloc_or_die(m_high + 1);
    memset(prev_key, 0, m_high + 1);
    size_t prev_len = 0;

    const char *key = NULL;
    size_t len = 0;

    while (!hattrie_iter_finished(i)) {
        memcpy(prev_key, key_copy, len);
        prev_key[len] = '\0';
        prev_len = len;
        ++count;

        key = hattrie_iter_key(i, &len);

        /* memory for key may be changed on iter, copy it */
        strncpy(key_copy, key, len);

        if (prev_key != NULL && cmpkey(prev_key, prev_len, key, len) > 0) {
            fprintf(stderr, "[error] iteration is not correctly ordered.\n");
            have_error = 1;
        }

        u = hattrie_iter_val(i);
        v = str_map_get(M, key, len);

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

        hattrie_iter_next(i);
    }

    if (count != M->m) {
        fprintf(stderr, "[error] iterated through %zu element, expected %zu\n",
                count, M->m);
        have_error = 1;        
    }

    hattrie_iter_free(i);
    free(prev_key);
    free(key_copy);

    fprintf(stderr, "done.\n");
}


void test_trie_non_ascii()
{
    fprintf(stderr, "checking non-ascii... \n");

    value_t* u;
    hattrie_t* T = hattrie_create();
    char* txt = "\x81\x70";

    u = hattrie_get(T, txt, strlen(txt));
    *u = 10;

    u = hattrie_tryget(T, txt, strlen(txt));
    if (*u != 10){
        fprintf(stderr, "can't store non-ascii strings\n");
        have_error = 1;
    }
    hattrie_free(T);

    fprintf(stderr, "done.\n");
}


typedef struct {
    int size;
    size_t lens[10];
    value_t vals[10];
} trie_walk_data_t;


static int trie_walk_cb(const char* key __attribute__((unused)), size_t len, value_t* val, void* data) {
    trie_walk_data_t* d = data;
    d->lens[d->size] = len;
    d->vals[d->size] = *val;
    d->size++;
    return hattrie_walk_continue;
}


void test_trie_walk()
{
    fprintf(stderr, "checking tryget_longest_match... \n");

    hattrie_t* T = hattrie_create();
    char* txt1 = "hello world1";
    char* txt2 = "hello world2";
    char* txt3 = "hello";
    value_t* val;

    val = hattrie_get(T, "", 0);
    *val = 4;
    val = hattrie_get(T, txt1, strlen(txt1));
    *val = 1;
    val = hattrie_get(T, txt2, strlen(txt2));
    *val = 2;
    val = hattrie_get(T, txt3, strlen(txt3));
    *val = 3;

#define EXPECT(check) \
    if (!(check)) {\
        fprintf(stderr, "[error] %s:%d: expect failure\n", __FILE__, __LINE__);\
        have_error = 1;\
    }

    trie_walk_data_t data = {
        .size = 0
    };
    char* txt = "hello world20";
    hattrie_walk(T, txt, strlen(txt), &data, trie_walk_cb);
    EXPECT(data.size == 3);
    EXPECT(data.lens[0] == 0);
    EXPECT(data.vals[0] == 4);
    EXPECT(data.lens[1] == strlen(txt3));
    EXPECT(data.vals[1] == 3);
    EXPECT(data.lens[2] == strlen(txt2));
    EXPECT(data.vals[2] == 2);

    data.size = 0;
    hattrie_walk(T, "", 0, &data, trie_walk_cb);
    EXPECT(data.size == 1);
    EXPECT(data.lens[0] == 0);
    EXPECT(data.vals[0] == 4);
#undef EXPECT

    hattrie_free(T);
}

void test_trie_walk_split_node_value()
{
    fprintf(stderr, "checking trie walk split node value... \n");

    hattrie_t* T = hattrie_create();
    value_t* val = hattrie_get(T, "a", 1);
    if (val == NULL) {
        fprintf(stderr, "[error] split node walk prefix insert failed\n");
        have_error = 1;
    }
    else {
        *val = 9;
    }

    char key[32];
    size_t i;
    for (i = 0; i < 17000; ++i) {
        snprintf(key, sizeof(key), "a%05zu", i);
        val = hattrie_get(T, key, strlen(key));
        if (val == NULL) {
            fprintf(stderr, "[error] split node walk filler insert failed\n");
            have_error = 1;
            break;
        }
        *val = 100 + i;
    }

    trie_walk_data_t data = {
        .size = 0
    };
    char* query = "a/not-present";
    hattrie_walk(T, query, strlen(query), &data, trie_walk_cb);

#define EXPECT(check) \
    if (!(check)) {\
        fprintf(stderr, "[error] %s:%d: expect failure\n", __FILE__, __LINE__);\
        have_error = 1;\
    }
    EXPECT(data.size == 1);
    EXPECT(data.lens[0] == 1);
    EXPECT(data.vals[0] == 9);
#undef EXPECT

    hattrie_free(T);
}

void fill_repeated(char* x, size_t len, char c)
{
    memset(x, c, len);
    x[len] = '\0';
}


void test_hattrie_key_length_limit()
{
    fprintf(stderr, "checking trie key length limit... \n");

    hattrie_t* T = hattrie_create();
    size_t max_trie_key_length = ahtable_max_key_length;
    char* max_key = malloc_or_die(max_trie_key_length + 1);
    char* too_long = malloc_or_die(max_trie_key_length + 2);
    fill_repeated(max_key, max_trie_key_length, 'm');
    fill_repeated(too_long, max_trie_key_length + 1, 'x');

    value_t* v = hattrie_get(T, max_key, max_trie_key_length);
    if (v == NULL) {
        fprintf(stderr, "[error] max length trie key was rejected\n");
        have_error = 1;
    }
    else {
        *v = 24;
        v = hattrie_tryget(T, max_key, max_trie_key_length);
        if (v == NULL || *v != 24) {
            fprintf(stderr, "[error] max length trie key was not retrieved\n");
            have_error = 1;
        }
    }
    hattrie_iter_t* iter = hattrie_iter_begin(T, true);
    if (hattrie_iter_finished(iter)) {
        fprintf(stderr, "[error] iterator missed max length trie key\n");
        have_error = 1;
    }
    else {
        size_t iter_len = 0;
        const char* iter_key = hattrie_iter_key(iter, &iter_len);
        if (iter_len != max_trie_key_length || memcmp(iter_key, max_key, max_trie_key_length) != 0) {
            fprintf(stderr, "[error] iterator returned max key length %zu, expected %zu\n",
                            iter_len, max_trie_key_length);
            have_error = 1;
        }
        hattrie_iter_next(iter);
        if (!hattrie_iter_finished(iter)) {
            fprintf(stderr, "[error] iterator returned extra key after max length trie key\n");
            have_error = 1;
        }
    }
    hattrie_iter_free(iter);

    v = hattrie_get(T, too_long, max_trie_key_length + 1);
    if (v != NULL) {
        fprintf(stderr, "[error] oversized trie key was inserted\n");
        have_error = 1;
    }
    if (hattrie_tryget(T, too_long, max_trie_key_length + 1) != NULL) {
        fprintf(stderr, "[error] oversized trie key was found\n");
        have_error = 1;
    }
    if (hattrie_del(T, too_long, max_trie_key_length + 1) == 0) {
        fprintf(stderr, "[error] oversized trie key was deleted\n");
        have_error = 1;
    }
    if (hattrie_size(T) != 1) {
        fprintf(stderr, "[error] oversized trie key changed size to %zu\n", hattrie_size(T));
        have_error = 1;
    }

    free(max_key);
    free(too_long);
    hattrie_free(T);

    fprintf(stderr, "done.\n");
}


void test_hattrie_fused_iteration()
{
    fprintf(stderr, "checking fused iterator reads... \n");

    hattrie_t* trie = hattrie_create();
    value_t* value = hattrie_get(trie, "", 0);
    if (value != NULL) *value = 3;
    value = hattrie_get(trie, "alpha", 5);
    if (value != NULL) *value = 5;
    value = hattrie_get(trie, "beta", 4);
    if (value != NULL) *value = 7;

    const char* expected_keys[] = {"", "alpha", "beta"};
    const size_t expected_lengths[] = {0, 5, 4};
    const value_t expected_values[] = {3, 5, 7};
    hattrie_iter_t* iter = hattrie_iter_begin(trie, true);
    const char* key = (const char*) 1;
    size_t len = 123;
    value_t iter_value = 99;
    size_t count = 0;
    bool live = hattrie_iter_read(iter, false, &key, &len, &iter_value);
    while (live) {
        if (count >= 3 || len != expected_lengths[count] ||
            memcmp(key, expected_keys[count], len) != 0 || iter_value != expected_values[count]) {
            fprintf(stderr, "[error] fused iterator returned an unexpected entry at %zu\n", count);
            have_error = 1;
            break;
        }
        ++count;
        live = hattrie_iter_read(iter, true, &key, &len, &iter_value);
    }
    if (count != 3 || key != NULL || len != 0 || iter_value != 0) {
        fprintf(stderr, "[error] fused iterator exhaustion state is invalid\n");
        have_error = 1;
    }
    hattrie_iter_free(iter);
    hattrie_free(trie);

    key = (const char*) 1;
    len = 123;
    iter_value = 99;
    if (hattrie_iter_read(NULL, false, &key, &len, &iter_value) || key != NULL || len != 0 || iter_value != 0) {
        fprintf(stderr, "[error] fused iterator accepted a null iterator\n");
        have_error = 1;
    }

    fprintf(stderr, "done.\n");
}

void test_hattrie_batch_iteration()
{
    fprintf(stderr, "checking batched iterator reads... \n");

    hattrie_t* trie = hattrie_create();
    const char* expected_keys[] = {"", "alpha", "beta", "long-key-value"};
    const size_t expected_lengths[] = {0, 5, 4, 14};
    size_t i;
    for (i = 0; i < 4; ++i) {
        value_t* value = hattrie_get(trie, expected_keys[i], expected_lengths[i]);
        if (value != NULL) *value = (value_t)(i + 1);
    }

    hattrie_iter_t* iter = hattrie_iter_begin(trie, true);
    hattrie_iter_record_t records[2];
    char small_keys[4];
    size_t required = 0;
    bool finished = true;
    size_t count = hattrie_iter_read_batch(iter, small_keys, sizeof(small_keys), records, 2, &required, &finished);
    if (count != 1 || records[0].key_len != 0 || records[0].value != 1 || required != 5 || finished) {
        fprintf(stderr, "[error] batched iterator small-buffer state is invalid\n");
        have_error = 1;
    }

    char keys[32];
    size_t seen = count;
    while (!hattrie_iter_finished(iter)) {
        required = 0;
        count = hattrie_iter_read_batch(iter, keys, sizeof(keys), records, 2, &required, &finished);
        if (count == 0 || required != 0) {
            fprintf(stderr, "[error] batched iterator made no progress\n");
            have_error = 1;
            break;
        }
        for (i = 0; i < count; ++i) {
            size_t expected = seen + i;
            if (expected >= 4 || records[i].key_len != expected_lengths[expected] ||
                memcmp(keys + records[i].key_offset, expected_keys[expected], records[i].key_len) != 0 ||
                records[i].value != (value_t)(expected + 1)) {
                fprintf(stderr, "[error] batched iterator returned an unexpected entry at %zu\n", expected);
                have_error = 1;
            }
        }
        seen += count;
    }
    if (seen != 4 || !finished || hattrie_iter_read_batch(iter, keys, sizeof(keys), records, 2, &required, &finished) != 0) {
        fprintf(stderr, "[error] batched iterator exhaustion state is invalid\n");
        have_error = 1;
    }
    hattrie_iter_free(iter);
    hattrie_free(trie);

    required = 99;
    if (hattrie_iter_read_batch(NULL, keys, sizeof(keys), records, 2, &required, &finished) != 0 || required != 0 || !finished) {
        fprintf(stderr, "[error] batched iterator accepted a null iterator\n");
        have_error = 1;
    }

    fprintf(stderr, "done.\n");
}


void test_hattrie_batch_lookup()
{
    fprintf(stderr, "checking batched trie lookups... \n");

    hattrie_t* trie = hattrie_create();
    value_t* value = hattrie_get(trie, "", 0);
    if (value != NULL) *value = 3;
    value = hattrie_get(trie, "alpha", 5);
    if (value != NULL) *value = 5;
    value = hattrie_get(trie, "beta", 4);
    if (value != NULL) *value = 7;

    const char keys[] = "alphamissingbeta";
    const hattrie_key_record_t records[] = {
        {0, 5},
        {5, 7},
        {12, 4},
        {0, 0},
    };
    value_t values[] = {99, 99, 99, 99};
    size_t count = hattrie_tryget_batch(trie, keys, sizeof(keys) - 1, records, 4, values);
    if (count != 4 || values[0] != 5 || values[1] != 0 || values[2] != 7 || values[3] != 3) {
        fprintf(stderr, "[error] batched trie lookup returned unexpected values\n");
        have_error = 1;
    }

    const hattrie_key_record_t invalid_records[] = {
        {0, 5},
        {sizeof(keys), 1},
        {12, 4},
    };
    value_t invalid_values[] = {99, 99, 99};
    count = hattrie_tryget_batch(trie, keys, sizeof(keys) - 1, invalid_records, 3, invalid_values);
    if (count != 1 || invalid_values[0] != 5 || invalid_values[1] != 0 || invalid_values[2] != 0) {
        fprintf(stderr, "[error] batched trie lookup did not stop safely at an invalid range\n");
        have_error = 1;
    }
    if (hattrie_tryget_batch(NULL, keys, sizeof(keys) - 1, records, 4, values) != 0 ||
        hattrie_tryget_batch(trie, NULL, 1, records, 4, values) != 0 ||
        hattrie_tryget_batch(trie, keys, sizeof(keys) - 1, NULL, 4, values) != 0 ||
        hattrie_tryget_batch(trie, keys, sizeof(keys) - 1, records, 4, NULL) != 0) {
        fprintf(stderr, "[error] batched trie lookup accepted invalid arguments\n");
        have_error = 1;
    }

    hattrie_free(trie);
    fprintf(stderr, "done.\n");
}


void test_hattrie_clear_resets_root()
{
    fprintf(stderr, "checking trie clear reset... \n");

    hattrie_t* T = hattrie_create();
    value_t* v = hattrie_get(T, "alpha", 5);
    if (v == NULL) {
        fprintf(stderr, "[error] trie clear test insert failed\n");
        have_error = 1;
    }
    else {
        *v = 31;
    }
    if (hattrie_size(T) != 1) {
        fprintf(stderr, "[error] trie size before clear is %zu, expected 1\n", hattrie_size(T));
        have_error = 1;
    }

    hattrie_clear(T);
    if (hattrie_size(T) != 0) {
        fprintf(stderr, "[error] trie size after clear is %zu, expected 0\n", hattrie_size(T));
        have_error = 1;
    }
    if (hattrie_tryget(T, "alpha", 5) != NULL) {
        fprintf(stderr, "[error] cleared trie key was still found\n");
        have_error = 1;
    }
    v = hattrie_get(T, "beta", 4);
    if (v == NULL) {
        fprintf(stderr, "[error] trie clear test insert after clear failed\n");
        have_error = 1;
    }
    else {
        *v = 41;
        v = hattrie_tryget(T, "beta", 4);
        if (v == NULL || *v != 41) {
            fprintf(stderr, "[error] trie clear test could not retrieve post-clear value\n");
            have_error = 1;
        }
    }

    hattrie_free(T);
    fprintf(stderr, "done.\n");
}


void test_hattrie_dup_copies_values()
{
    fprintf(stderr, "checking trie duplication... \n");

    if (hattrie_dup(NULL) != NULL) {
        fprintf(stderr, "[error] null trie duplicate returned a trie\n");
        have_error = 1;
    }

    hattrie_t* empty = hattrie_create();
    hattrie_t* empty_copy = hattrie_dup(empty);
    if (empty_copy == NULL) {
        fprintf(stderr, "[error] empty trie duplicate returned NULL\n");
        have_error = 1;
    }
    else if (hattrie_size(empty_copy) != 0) {
        fprintf(stderr, "[error] empty trie duplicate size is %zu, expected 0\n", hattrie_size(empty_copy));
        have_error = 1;
    }
    hattrie_free(empty);
    hattrie_free(empty_copy);

    hattrie_t* T = hattrie_create();
    value_t* v = hattrie_get(T, "", 0);
    if (v == NULL) {
        fprintf(stderr, "[error] duplicate test root insert failed\n");
        have_error = 1;
    }
    else {
        *v = 7;
    }
    v = hattrie_get(T, "alpha", 5);
    if (v == NULL) {
        fprintf(stderr, "[error] duplicate test alpha insert failed\n");
        have_error = 1;
    }
    else {
        *v = 11;
    }
    v = hattrie_get(T, "prefix:beta", 11);
    if (v == NULL) {
        fprintf(stderr, "[error] duplicate test beta insert failed\n");
        have_error = 1;
    }
    else {
        *v = 13;
    }

    hattrie_t* copy = hattrie_dup(T);
    if (copy == NULL) {
        fprintf(stderr, "[error] populated trie duplicate returned NULL\n");
        have_error = 1;
    }
    else {
        if (hattrie_size(copy) != hattrie_size(T)) {
            fprintf(stderr, "[error] duplicate trie size is %zu, expected %zu\n", hattrie_size(copy), hattrie_size(T));
            have_error = 1;
        }
        v = hattrie_tryget(copy, "", 0);
        if (v == NULL || *v != 7) {
            fprintf(stderr, "[error] duplicate trie root value = %lu, expected 7\n", v == NULL ? 0 : *v);
            have_error = 1;
        }
        v = hattrie_tryget(copy, "alpha", 5);
        if (v == NULL || *v != 11) {
            fprintf(stderr, "[error] duplicate trie alpha value = %lu, expected 11\n", v == NULL ? 0 : *v);
            have_error = 1;
        }
        v = hattrie_tryget(copy, "prefix:beta", 11);
        if (v == NULL || *v != 13) {
            fprintf(stderr, "[error] duplicate trie beta value = %lu, expected 13\n", v == NULL ? 0 : *v);
            have_error = 1;
        }

        hattrie_del(T, "alpha", 5);
        v = hattrie_get(T, "", 0);
        if (v != NULL) *v = 99;
        v = hattrie_get(T, "original-only", 13);
        if (v != NULL) *v = 17;

        v = hattrie_tryget(copy, "", 0);
        if (v == NULL || *v != 7) {
            fprintf(stderr, "[error] duplicate root changed after original mutation\n");
            have_error = 1;
        }
        v = hattrie_tryget(copy, "alpha", 5);
        if (v == NULL || *v != 11) {
            fprintf(stderr, "[error] duplicate alpha changed after original delete\n");
            have_error = 1;
        }
        if (hattrie_tryget(copy, "original-only", 13) != NULL) {
            fprintf(stderr, "[error] duplicate saw key inserted only in original\n");
            have_error = 1;
        }

        hattrie_del(copy, "prefix:beta", 11);
        v = hattrie_get(copy, "copy-only", 9);
        if (v != NULL) *v = 19;

        v = hattrie_tryget(T, "prefix:beta", 11);
        if (v == NULL || *v != 13) {
            fprintf(stderr, "[error] original beta changed after duplicate delete\n");
            have_error = 1;
        }
        if (hattrie_tryget(T, "copy-only", 9) != NULL) {
            fprintf(stderr, "[error] original saw key inserted only in duplicate\n");
            have_error = 1;
        }
    }

    hattrie_free(copy);
    hattrie_free(T);
    fprintf(stderr, "done.\n");
}


void test_hattrie_null_cleanup()
{
    fprintf(stderr, "checking trie null api... \n");

    if (hattrie_size(NULL) != 0) {
        fprintf(stderr, "[error] null trie size was not zero\n");
        have_error = 1;
    }
    if (hattrie_get(NULL, "missing", 7) != NULL) {
        fprintf(stderr, "[error] null trie get returned a value\n");
        have_error = 1;
    }
    if (hattrie_tryget(NULL, "missing", 7) != NULL) {
        fprintf(stderr, "[error] null trie tryget returned a value\n");
        have_error = 1;
    }
    if (hattrie_del(NULL, "missing", 7) == 0) {
        fprintf(stderr, "[error] null trie delete reported success\n");
        have_error = 1;
    }

    trie_walk_data_t data = { .size = 0 };
    hattrie_walk(NULL, "missing", 7, &data, trie_walk_cb);
    if (data.size != 0) {
        fprintf(stderr, "[error] null trie walk invoked callback\n");
        have_error = 1;
    }

    hattrie_iter_t* iter = hattrie_iter_begin(NULL, false);
    if (!hattrie_iter_finished(iter)) {
        fprintf(stderr, "[error] null trie iterator was not finished\n");
        have_error = 1;
    }
    size_t len = 123;
    if (hattrie_iter_key(iter, &len) != NULL || len != 0) {
        fprintf(stderr, "[error] null trie iterator returned a key\n");
        have_error = 1;
    }
    if (hattrie_iter_val(iter) != NULL) {
        fprintf(stderr, "[error] null trie iterator returned a value\n");
        have_error = 1;
    }
    hattrie_iter_next(iter);
    if (!hattrie_iter_finished(iter)) {
        fprintf(stderr, "[error] null trie iterator advanced to a value\n");
        have_error = 1;
    }
    hattrie_iter_free(iter);

    iter = hattrie_iter_with_prefix(NULL, true, "prefix", 6);
    if (!hattrie_iter_finished(iter)) {
        fprintf(stderr, "[error] null trie prefixed iterator was not finished\n");
        have_error = 1;
    }
    hattrie_iter_free(iter);

    hattrie_iter_next(NULL);
    if (!hattrie_iter_finished(NULL)) {
        fprintf(stderr, "[error] null iterator was not finished\n");
        have_error = 1;
    }
    len = 123;
    if (hattrie_iter_key(NULL, &len) != NULL || len != 0) {
        fprintf(stderr, "[error] null iterator returned a key\n");
        have_error = 1;
    }
    if (hattrie_iter_val(NULL) != NULL) {
        fprintf(stderr, "[error] null iterator returned a value\n");
        have_error = 1;
    }
    hattrie_iter_free(NULL);

    hattrie_t* T = hattrie_create();
    if (hattrie_get(T, NULL, 1) != NULL) {
        fprintf(stderr, "[error] invalid trie key get returned a value\n");
        have_error = 1;
    }
    if (hattrie_tryget(T, NULL, 1) != NULL) {
        fprintf(stderr, "[error] invalid trie key tryget returned a value\n");
        have_error = 1;
    }
    if (hattrie_del(T, NULL, 1) == 0) {
        fprintf(stderr, "[error] invalid trie key delete reported success\n");
        have_error = 1;
    }
    data.size = 0;
    hattrie_walk(T, NULL, 1, &data, trie_walk_cb);
    if (data.size != 0) {
        fprintf(stderr, "[error] invalid trie key walk invoked callback\n");
        have_error = 1;
    }
    hattrie_walk(T, "", 0, &data, NULL);
    iter = hattrie_iter_with_prefix(T, true, NULL, 1);
    if (!hattrie_iter_finished(iter)) {
        fprintf(stderr, "[error] invalid prefix trie iterator was not finished\n");
        have_error = 1;
    }
    hattrie_iter_free(iter);

    value_t* v = hattrie_get(T, "", 0);
    if (v != NULL) *v = 5;
    v = hattrie_get(T, "alpha", 5);
    if (v != NULL) *v = 7;
    iter = hattrie_iter_begin(T, true);
    while (!hattrie_iter_finished(iter)) {
        if (hattrie_iter_key(iter, NULL) == NULL) {
            fprintf(stderr, "[error] live trie iterator returned NULL key without length output\n");
            have_error = 1;
        }
        hattrie_iter_next(iter);
    }
    hattrie_iter_free(iter);
    hattrie_free(T);

    hattrie_clear(NULL);
    hattrie_free(NULL);

    fprintf(stderr, "done.\n");
}



int main()
{
    test_trie_non_ascii();
    test_trie_walk();
    test_trie_walk_split_node_value();
    test_hattrie_null_cleanup();
    test_hattrie_dup_copies_values();
    test_hattrie_clear_resets_root();
    test_hattrie_key_length_limit();
    test_hattrie_fused_iteration();
    test_hattrie_batch_iteration();
    test_hattrie_batch_lookup();

    setup();
    test_hattrie_insert();
    test_hattrie_iteration();
    teardown();

    setup();
    test_hattrie_insert();
    test_hattrie_sorted_iteration();
    teardown();

    if (have_error) {
        return -1;
    }
    return 0;
}
