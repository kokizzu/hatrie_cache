/*
 * This file is part of hat-trie.
 *
 * Copyright (c) 2011 by Daniel C. Jones <dcjones@cs.washington.edu>
 *
 */

#include "misc.h"
#include <stdlib.h>


int checked_array_size(size_t n, size_t size, size_t* out)
{
    if (size != 0 && n > ((size_t) -1) / size) {
        return -1;
    }
    *out = n * size;
    return 0;
}


size_t size_add_or_die(size_t a, size_t b)
{
    if (a > ((size_t) -1) - b) {
        fprintf(stderr, "Cannot allocate %zu plus %zu bytes.\n", a, b);
        exit(EXIT_FAILURE);
    }
    return a + b;
}


size_t array_bytes_or_die(size_t n, size_t size)
{
    size_t bytes;
    if (checked_array_size(n, size, &bytes) != 0) {
        fprintf(stderr, "Cannot allocate array of %zu elements of %zu bytes.\n", n, size);
        exit(EXIT_FAILURE);
    }
    return bytes;
}


void* malloc_or_die(size_t n)
{
    void* p = malloc(n);
    if (p == NULL && n != 0) {
        fprintf(stderr, "Cannot allocate %zu bytes.\n", n);
        exit(EXIT_FAILURE);
    }
    return p;
}


void* malloc_array_or_die(size_t n, size_t size)
{
    return malloc_or_die(array_bytes_or_die(n, size));
}


void* realloc_or_die(void* ptr, size_t n)
{
    void* p = realloc(ptr, n);
    if (p == NULL && n != 0) {
        fprintf(stderr, "Cannot allocate %zu bytes.\n", n);
        exit(EXIT_FAILURE);
    }
    return p;
}


void* realloc_array_or_die(void* ptr, size_t n, size_t size)
{
    return realloc_or_die(ptr, array_bytes_or_die(n, size));
}


FILE* fopen_or_die(const char* path, const char* mode)
{
    FILE* f = fopen(path, mode);
    if (f == NULL) {
        fprintf(stderr, "Cannot open file %s with mode %s.\n", path, mode);
        exit(EXIT_FAILURE);
    }
    return f;
}


