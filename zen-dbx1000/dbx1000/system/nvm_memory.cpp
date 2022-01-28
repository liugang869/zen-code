#include"nvm_memory.h"
//static inline void clflush(void * addr);
//static inline void sfence(void);

void *nvm_mem_alloc(long size){
#if REAL_NVM
    printf("real nvm!!!size = %ld\n", size);
    size_t mapped_len;
    int is_pmem;
    void * addr = pmem_map_file(PATH, size, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);//map	
    //printf("addr = %p, size = %d, mapped_len = %lu, is_pmem = %d\n", addr, size, mapped_len, is_pmem);
    //exit(0);
#else
    //printf("numa1 as nvm!!!\n");
    void * addr = numa_alloc_onnode(size, 1);
#endif
    if(addr == NULL){
    	printf("allocation failed!\n");
    	exit(1);
    } else
    	return addr;
}

void *dram_mem_alloc(int size){
    void *addr = numa_alloc_onnode(size, 0);
    return addr;
}

void nvm_mem_free(void *p, int size){
#if REAL_NVM
    pmem_unmap(p, size); //unmap
#else
    numa_free(p, size);
#endif
}

void dram_mem_free(void *p, int size){
    numa_free(p, size);
}

