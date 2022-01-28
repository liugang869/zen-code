#pragma once 

#include "global.h"
#include "helper.h"
//cly begin
#include <pthread.h>
#define TABLE_STORE_SIZE 0x1000000000//not sure yet, storage size for each table
#define MAX_FREE_ARRAY_SIZE 0x1000
#define TB_CACHE_SIZE 0x800
//cly end
// TODO sequential scan is not supported yet.
// only index access is supported for table. 
class Catalog;
class row_t;
//cly begin
class INDEX;
class nvm_row;

class nvm_table{
  private:
    uint64_t tuple_size;
    uint64_t assigned_size; //not sure if needed
    char * start_addr;
    char * end_addr;
    pthread_mutex_t thread_alloc_lock;
    struct nvm_free_tuple_array {
        uint64_t cur_available;
        nvm_row * free_tuple_array[MAX_FREE_ARRAY_SIZE];
    };
    nvm_free_tuple_array * fta_per_thread;
    char *cur_position;
  public:
    nvm_table(uint64_t tuple_size, uint64_t assigned_size);
    nvm_row * get_nvm_addr(uint64_t thread_id);
    void refill_free_addr(struct nvm_free_tuple_array &fta);
    void write_row_to_nvm(uint64_t row_id, uint64_t thread_id, row_t * dram_row);
};
//cly end

class table_t
{
public:
	void init(Catalog * schema);
	// row lookup should be done with index. But index does not have
	// records for new rows. get_new_row returns the pointer to a 
	// new row.	
	RC get_new_row(row_t *& row); // this is equivalent to insert()
	RC get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id);

	void delete_row(); // TODO delete_row is not supportet yet

	uint64_t get_table_size() { return cur_tab_size; };
	Catalog * get_schema() { return schema; };
	const char * get_table_name() { return table_name; };
    //cly begin
    nvm_table * get_nvm_table() { return nvm_table_; };
    void free_orig_row(row_t * row, uint64_t part_id, uint64_t &row_id);
    row_t * get_row_from_cache(INDEX * index, uint64_t part_id, uint64_t &row_id, uint64_t thd_id);
    //cly end

	Catalog * 		schema;
private:
    // cly begin
    struct cache_bd {
        uint64_t lw_bd;
        uint64_t up_bd;
        uint64_t thd_cur;
    };
    cache_bd * bd_per_thd;
    // cly end
	const char * 	table_name;
	uint64_t  		cur_tab_size;
    //cly begin
    nvm_table *     nvm_table_;
    row_t *         tb_cache;
    //uint64_t        cache_cursor;
	//char 			pad[CL_SIZE - sizeof(void *)*3];
	char 			pad[CL_SIZE - sizeof(void *)*6];
    //cly end
};
