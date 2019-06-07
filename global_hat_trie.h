
#pragma once
#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

int HatInsert(const char* key, const int64_t value);

int HatErase(const char* key);

int64_t HatFind(const char* key);

#ifdef __cplusplus
}  // extern "C"
#endif