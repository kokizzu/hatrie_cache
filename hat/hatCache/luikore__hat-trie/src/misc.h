/*
 * This file is part of hat-trie.
 *
 * Copyright (c) 2011 by Daniel C. Jones <dcjones@cs.washington.edu>
 *
 * misc :
 * miscelaneous functions.
 *
 */

#ifndef LINESET_MISC_H
#define LINESET_MISC_H

#include <stdio.h>
#include <stdlib.h>

int checked_array_size(size_t, size_t, size_t*);
size_t size_add_or_die(size_t, size_t);
size_t array_bytes_or_die(size_t, size_t);
void* malloc_or_die(size_t);
void* malloc_array_or_die(size_t, size_t);
void* realloc_or_die(void*, size_t);
void* realloc_array_or_die(void*, size_t, size_t);
FILE* fopen_or_die(const char*, const char*);

#endif
