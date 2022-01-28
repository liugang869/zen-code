#include "global.h"
#include "helper.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "mem_alloc.h"
//cly add
#include "nvm_memory.h"
#include "index_hash.h"
#include "index_btree.h"

//cly begin
nvm_table::nvm_table(uint64_t tuple_size, uint64_t assigned_size){
    printf("assigned size = %lu\n", assigned_size);
    this->start_addr = (char *)nvm_mem_alloc(assigned_size);
    //memset(this->start_addr, 0, assigned_size);
    this->cur_position = this->start_addr;
    this->tuple_size = tuple_size;
    this->assigned_size = assigned_size;
    this->end_addr = start_addr + assigned_size - tuple_size;
    this->fta_per_thread = new nvm_free_tuple_array[g_thread_cnt];
    pthread_mutex_init(&(this->thread_alloc_lock), NULL);
    // TODO the difference between using a mutex lock, a ptr to mutex lock, and a volatile bool value?
    for(uint64_t i = 0; i< g_thread_cnt; i++){
        nvm_free_tuple_array &this_fta = fta_per_thread[i];
        this_fta.cur_available = 0;
        refill_free_addr(this_fta);
    }
}

nvm_row * nvm_table::get_nvm_addr(uint64_t thread_id){
    struct nvm_free_tuple_array &this_fta = fta_per_thread[thread_id];
    nvm_row * nvm_ptr = this_fta.free_tuple_array[this_fta.cur_available];
    if(this_fta.cur_available == 0){
        refill_free_addr(this_fta);
    } else {
        this_fta.cur_available--;
    }
    //check if nvm_ptr is null as a test
    return nvm_ptr;
}

void nvm_table::refill_free_addr(struct nvm_free_tuple_array &fta){
    uint64_t refill_cnt = MAX_FREE_ARRAY_SIZE>>1;
    pthread_mutex_lock(&(this->thread_alloc_lock)); // lock
    for(uint64_t i = 0; i< refill_cnt; i++){
        if(cur_position >= end_addr){
            // no page-like allocation so if the position exceed the end_addr, the system should report the error
            // need to find a proper "table size"
            printf("too much nvm record!");
            printf("cur_position: %p, start_addr: %p, end_addr: %p\n", cur_position, start_addr, end_addr);
            assert(false);
        } else {
            fta.free_tuple_array[i] = reinterpret_cast<nvm_row *>(cur_position);
            cur_position += this->tuple_size + sizeof(ts_t) + sizeof(uint64_t);
        }
    }
    fta.cur_available = refill_cnt - 1;
    pthread_mutex_unlock(&(this->thread_alloc_lock)); //unlock 
}
//cly end

void table_t::init(Catalog * schema) {
	this->table_name = schema->table_name;
	this->schema = schema;
    //cly begin
    this->nvm_table_ = new nvm_table(schema->get_tuple_size(), TABLE_STORE_SIZE);//initialization of nvm table
    this->tb_cache = (row_t *)_mm_malloc(sizeof(row_t) * TB_CACHE_SIZE, 64);
    for(int i = 0; i < TB_CACHE_SIZE; i++){
        tb_cache[i].ref_cnt = -1;
        //tb_cache[i].status = NOUSE; // tb_cache[i] is a row_t 
        tb_cache[i].read = 0;
    }
    this->bd_per_thd = new cache_bd[g_thread_cnt];
    uint64_t size_per_thd = TB_CACHE_SIZE/g_thread_cnt; 
    for(uint64_t i = 0; i < g_thread_cnt; i++){
        bd_per_thd[i].lw_bd = i * size_per_thd;
        bd_per_thd[i].up_bd = bd_per_thd[i].lw_bd + size_per_thd - 1;
        bd_per_thd[i].thd_cur = bd_per_thd[i].lw_bd;
    }
    //cly end
}

RC table_t::get_new_row(row_t *& row) {
	// this function is obsolete. 
	assert(false);
	return RCOK;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id) {
	RC rc = RCOK;
	//cly delete: cur_tab_size ++;
	
	row = (row_t *) _mm_malloc(sizeof(row_t), 64);
    rc = row->init(this, part_id, row_id);
	//cly delete: row->init_manager(row);
	
    return rc;
}

void table_t::free_orig_row(row_t * row, uint64_t part_id, uint64_t &row_id){
    row->free_manager();
    //initialize a bunch of things
    row->free_row();
    row->persistent_row = NULL;
    row->init(this, part_id, row_id);
}

row_t * table_t::get_row_from_cache(INDEX * index, uint64_t part_id, uint64_t &row_id, uint64_t thd_id){
    row_t * ret_row = NULL;
    //uint64_t this_cursor = ATOM_FETCH_ADD(cache_cursor, 1);
    while(ret_row == NULL){
        uint64_t cursor = bd_per_thd[thd_id].thd_cur;
        //printf("cursor = %lu, ref_cnt = %ld\n", cursor, tb_cache[cursor].ref_cnt);
        if (tb_cache[cursor].ref_cnt == -1) {
            //printf("cache null\n");
            tb_cache[cursor].init(this, part_id, row_id);
            ret_row = &tb_cache[cursor];
        } else if (tb_cache[cursor].ref_cnt == 0){
            if(tb_cache[cursor].read == 1){
                tb_cache[cursor].read--;
            } else {
                assert(tb_cache[cursor].read == 0);
                //index_replace
                //printf("cache aborted/committed\n");
                row_t * victim_row = &tb_cache[cursor];
                uint64_t key = victim_row->get_primary_key();
                uint64_t victim_part_id = victim_row->get_part_id();
                index->index_get_latch(victim_row->get_primary_key(), victim_row->get_part_id());
                if(victim_row->ref_cnt != 0){
                    index->index_release_latch(key, victim_part_id); 
                    bd_per_thd[thd_id].thd_cur++;
                    if(bd_per_thd[thd_id].thd_cur >= bd_per_thd[thd_id].up_bd)
                        bd_per_thd[thd_id].thd_cur = bd_per_thd[thd_id].lw_bd;
                    continue;
                }
                assert(victim_row->ref_cnt == 0);
                itemid_t * item = (itemid_t *) mem_allocator.alloc(sizeof(itemid_t), victim_row->get_part_id());
                item->init();
                item->type = DT_row;
                item->location = victim_row->persistent_row;
                item->valid = true;
                index->index_replace_without_getting_latch(victim_row->get_primary_key(), item, victim_row->get_part_id());
                index->index_release_latch(key, victim_part_id); 
                free_orig_row(victim_row, part_id, row_id);
                ret_row = victim_row;
                INC_GLOB_STATS(switch_cnt, 1);
            }
        }
        bd_per_thd[thd_id].thd_cur++;
        if(bd_per_thd[thd_id].thd_cur >= bd_per_thd[thd_id].up_bd)
            bd_per_thd[thd_id].thd_cur = bd_per_thd[thd_id].lw_bd;
    }
    return ret_row;
}
