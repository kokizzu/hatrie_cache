
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
    EXPECT(data.size == 2);
    EXPECT(data.lens[0] == strlen(txt3));
    EXPECT(data.vals[0] == 3);
    EXPECT(data.lens[1] == strlen(txt2));
    EXPECT(data.vals[1] == 2);
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
    hattrie_free(T);

    hattrie_clear(NULL);
    hattrie_free(NULL);

    fprintf(stderr, "done.\n");
}



int main()
{
    test_trie_non_ascii();
    test_trie_walk();
    test_hattrie_null_cleanup();
    test_hattrie_clear_resets_root();
    test_hattrie_key_length_limit();

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
