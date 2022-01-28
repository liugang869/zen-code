#pragma once

#include <numa.h>
#include <libpmem.h>
#include <stdio.h>
#include "global.h"
#define DRAM_TAG 0x8000000000000000
#define DRAM_MASK 0x7fffffffffffffff
#define PATH "/mnt/mypmem0/chenleying/dbx1000_fileE"

void *nvm_mem_alloc(long size);
void *dram_mem_alloc(int size);
void nvm_mem_free(void *p, int size);
void dram_mem_free(void *p, int size);
