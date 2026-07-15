
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
    test_ahtable_delete_releases_empty_slot();
    test_ahtable_delete_compacts_nonempty_slot();
    test_ahtable_clear_resets_size();
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
