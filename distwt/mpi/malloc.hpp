#pragma once

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <cstdlib>

class malloc_callback {
public:
    static void (*on_alloc)(size_t);
    static void (*on_free)(size_t);
};

extern "C" void* __libc_malloc(size_t);
extern "C" void  __libc_free(void*);
extern "C" void* __libc_realloc(void*, size_t);
