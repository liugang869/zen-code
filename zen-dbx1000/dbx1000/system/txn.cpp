#include "txn.h"
#include "row.h"
#include "wl.h"
#include "ycsb.h"
#include "thread.h"
#include "mem_alloc.h"
#include "occ.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "nvm_memory.h"

static inline void clflush(void * addr){
    asm volatile("clflush %0"::"m"(*((char *)addr)));
}

static inline void sfence(void){
    asm volatile("sfence");
}

void txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	this->h_thd = h_thd;
	this->h_wl = h_wl;
	pthread_mutex_init(&txn_lock, NULL);
	lock_ready = false;
	ready_part = 0;
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
	accesses = (Access **) _mm_malloc(sizeof(Access *) * MAX_ROW_PER_TXN, 64);
	for (int i = 0; i < MAX_ROW_PER_TXN; i++)
		accesses[i] = NULL;
	num_accesses_alloc = 0;
#if CC_ALG == TICTOC || CC_ALG == SILO
	_pre_abort = (g_params["pre_abort"] == "true");
	if (g_params["validation_lock"] == "no-wait")
		_validation_no_wait = true;
	else if (g_params["validation_lock"] == "waiting")
		_validation_no_wait = false;
	else 
		assert(false);
#endif
#if CC_ALG == TICTOC
	_max_wts = 0;
	_write_copy_ptr = (g_params["write_copy_form"] == "ptr");
	_atomic_timestamp = (g_params["atomic_timestamp"] == "true");
#elif CC_ALG == SILO
	_cur_tid = 0;
#endif

}

void txn_man::set_txn_id(txnid_t txn_id) {
	this->txn_id = txn_id;
}

txnid_t txn_man::get_txn_id() {
	return this->txn_id;
}

workload * txn_man::get_wl() {
	return h_wl;
}

uint64_t txn_man::get_thd_id() {
	return h_thd->get_thd_id();
}

void txn_man::set_ts(ts_t timestamp) {
	this->timestamp = timestamp;
}

ts_t txn_man::get_ts() {
	return this->timestamp;
}

void txn_man::cleanup(RC rc) {
#if CC_ALG == HEKATON
	// cly delete: row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
    // cly begin
    persist(rc);
    set_row_status(rc);
    // cly end
	return;
#endif
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		row_t * orig_r = accesses[rid]->orig_row;
		access_t type = accesses[rid]->type;
		if (type == WR && rc == Abort)
			type = XP;

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
		if (type == RD) {
			accesses[rid]->data = NULL;
			continue;
		}
#endif

		if (ROLL_BACK && type == XP &&
					(CC_ALG == DL_DETECT || 
					CC_ALG == NO_WAIT || 
					CC_ALG == WAIT_DIE)) 
		{
			orig_r->return_row(type, this, accesses[rid]->orig_data);
		} else {
			orig_r->return_row(type, this, accesses[rid]->data);
		}
#if CC_ALG != TICTOC && CC_ALG != SILO
		//cly delete: 
        //accesses[rid]->data = NULL;
#endif
	}

	if (rc == Abort) {
		for (UInt32 i = 0; i < insert_cnt; i ++) {
			row_t * row = insert_rows[i];
			assert(g_part_alloc == false);
#if CC_ALG != HSTORE && CC_ALG != OCC
			mem_allocator.free(row->manager, 0);
#endif
			row->free_row();
			mem_allocator.free(row, sizeof(row));
            // cly add
            insert_rows[i] = NULL;
            // TODO move the aborted row out of index here? 
		}  
    }
    
    //cly delete: row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
#if CC_ALG == DL_DETECT
	dl_detector.clear_dep(get_txn_id());
#endif
    //cly begin: TODO check if this can apply to every cc method
    persist(rc);
    set_row_status(rc);
    //cly end
}

row_t * txn_man::get_row(row_t * row, access_t type, INDEX * index, idx_key_t key) {
    row->read = 1;
    //if (CC_ALG == HSTORE)
	//	return row;
	uint64_t starttime = get_sys_clock();
	RC rc = RCOK;
	if (accesses[row_cnt] == NULL) {
		Access * access = (Access *) _mm_malloc(sizeof(Access), 64);
		accesses[row_cnt] = access;
#if (CC_ALG == SILO || CC_ALG == TICTOC)
		access->data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->data->init(MAX_TUPLE_SIZE);
		access->orig_data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->orig_data->init(MAX_TUPLE_SIZE);
#elif (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
		access->orig_data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->orig_data->init(MAX_TUPLE_SIZE);
#endif
		num_accesses_alloc ++;
    }
// cly add
#if (CC_ALG == HSTORE)
    accesses[row_cnt]->data = row;
    accesses[row_cnt]->orig_row = row;
    accesses[row_cnt]->type = type;
    row_cnt++;
    return accesses[row_cnt - 1]->data;
#endif
	rc = row->get_row(type, this, accesses[ row_cnt ]->data);


	if (rc == Abort) {
		return NULL;
	}
	accesses[row_cnt]->orig_row = row;
	accesses[row_cnt]->type = type;
    // cly begin
    // accesses[row_cnt]->index = index;
    // accesses[row_cnt]->key = key;
    // cly end
#if CC_ALG == TICTOC
	accesses[row_cnt]->wts = last_wts;
	accesses[row_cnt]->rts = last_rts;
#elif CC_ALG == SILO
	accesses[row_cnt]->tid = last_tid;
#elif CC_ALG == HEKATON
	accesses[row_cnt]->history_entry = history_entry;
#endif

#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
	if (type == WR) {
		accesses[row_cnt]->orig_data->table = row->get_table();
		accesses[row_cnt]->orig_data->copy(row);
	}
#endif

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
	if (type == RD)
		row->return_row(type, this, accesses[ row_cnt ]->data);
#endif
	
	row_cnt ++;
	if (type == WR)
		wr_cnt ++;

	uint64_t timespan = get_sys_clock() - starttime;
	INC_TMP_STATS(get_thd_id(), time_man, timespan);
	return accesses[row_cnt - 1]->data;
}

// cly begin

void txn_man::index_insert(INDEX * index, idx_key_t key, row_t * row, int part_id){
    uint64_t pid = part_id;
    if (part_id == -1)
        pid = get_part_id(row);
    itemid_t * item = 
        (itemid_t *) mem_allocator.alloc( sizeof(itemid_t), pid );
    item->init();
    item->type = DT_row;
    item->location = row;
    item->valid = true;
    assert( index->index_insert(key, item, pid) == RCOK );
}

row_t * txn_man::get_row(nvm_row * nvm_row, access_t type, INDEX * index, idx_key_t key, int part_id){
    //1. get table from nvm_row
    table_t * table = index->table;
    assert(table);
    //2. use the table's get_new_row to create a dram version for the nvm row
    //   copy the data and the primary key
    row_t * dram_row;
    uint64_t row_id;
    dram_row = table->get_row_from_cache(index, part_id, row_id, get_thd_id());
    //try table->get_new_row(dram_row, 0, row_id);
    assert(dram_row);
    dram_row->copy_from_nvm(nvm_row);
    dram_row->init_manager(dram_row, nvm_row->nvm_wts);
#if CC_ALG != HSTORE
    assert(dram_row->manager);
#endif
    dram_row->persistent_row = nvm_row;
    //3. change the index
    itemid_t * item = (itemid_t *)mem_allocator.alloc(sizeof(itemid_t), part_id); 
    item->init();
    item->type = DT_row;
    item->location = (row_t *)((uint64_t)dram_row | DRAM_TAG);
    item->valid = true;
    index->index_replace_without_getting_latch(key, item, part_id);  
    // release the lock
    ATOM_ADD(dram_row->ref_cnt, 1);
    index->index_release_latch(key, part_id);
    //4. call dram_row's get_row
    return get_row(dram_row, type, index, key);
}

// cly end

void txn_man::insert_row(row_t * row, table_t * table) {
	if (CC_ALG == HSTORE)
		return;
	assert(insert_cnt < MAX_ROW_PER_TXN);
	insert_rows[insert_cnt ++] = row;
}

itemid_t *
txn_man::index_read(INDEX * index, idx_key_t key, int part_id) {
	uint64_t starttime = get_sys_clock();
	itemid_t * item;
	index->index_get_latch(key, part_id);
    index->index_read(key, item, part_id, get_thd_id());
    if((uint64_t)item->location & DRAM_TAG) { // DRAM
        row_t * row = (row_t *)((uint64_t)item->location & DRAM_MASK);
        ATOM_ADD(row->ref_cnt, 1);
        index->index_release_latch(key, part_id);
    } else { // NVM
        ;
    }
	INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
	return item;
}

void 
txn_man::index_read(INDEX * index, idx_key_t key, int part_id, itemid_t *& item) {
	uint64_t starttime = get_sys_clock();
	index->index_get_latch(key, part_id);
	index->index_read(key, item, part_id, get_thd_id());
    if((uint64_t)item->location & DRAM_TAG) { // DRAM
        row_t * row = (row_t *)((uint64_t)item->location & DRAM_MASK);
        ATOM_ADD(row->ref_cnt, 1);
        index->index_release_latch(key, part_id);
    } else { // NVM
        ;
    }
	INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
}

RC txn_man::finish(RC rc) {
#if CC_ALG == HSTORE
    //printf("finish with rc = %d\n", rc);
    persist(rc);
    set_row_status(rc);
	return RCOK;
#endif
	uint64_t starttime = get_sys_clock();
#if CC_ALG == OCC
	if (rc == RCOK)
		rc = occ_man.validate(this);
	else 
		cleanup(rc);
#elif CC_ALG == TICTOC
	if (rc == RCOK)
		rc = validate_tictoc();
	else 
		cleanup(rc);
#elif CC_ALG == SILO
	if (rc == RCOK)
		rc = validate_silo();
	else 
		cleanup(rc);
#elif CC_ALG == HEKATON
	rc = validate_hekaton(rc);
	cleanup(rc);
#else 
	cleanup(rc);
#endif
	uint64_t timespan = get_sys_clock() - starttime;
	INC_TMP_STATS(get_thd_id(), time_man,  timespan);
	INC_STATS(get_thd_id(), time_cleanup,  timespan);
	return rc;
}

void
txn_man::release() {
	for (int i = 0; i < num_accesses_alloc; i++)
		mem_allocator.free(accesses[i], 0);
	mem_allocator.free(accesses, 0);
}

//cly begin
RC txn_man::persist(RC rc){
    //printf("ever been to persist\n");
    for (int rid = row_cnt - 1; rid >= 0; rid--){
        if(accesses[rid]->type == WR && rc == RCOK){
            //TODO check if this need lock, because accesses[]->data is only a pointer
            row_t * orig_row = accesses[rid]->orig_row;
            table_t *tb = orig_row->get_table();
            nvm_row * dest_row = tb->get_nvm_table()->get_nvm_addr(get_thd_id());
            accesses[rid]->data->copy_to_nvm(dest_row, orig_row->get_tuple_size(), orig_row->get_primary_key(), this->get_ts());
            //printf("nvm_wts = %lu\n", dest_row->nvm_wts);
	    char * addr = (char *)dest_row;
	    int tot_size = orig_row->get_tuple_size() + sizeof(ts_t) + sizeof(uint64_t);
#if REAL_NVM
	    pmem_persist(addr, tot_size);
#else
            char * end_addr = addr + tot_size;
            for(; addr<end_addr; addr+=64){
                clflush(addr);
            }
#endif

            orig_row->persistent_row = dest_row;
        }
#if CC_ALG != TICTOC && CC_ALG != SILO
        accesses[rid]->data = NULL;
#endif
        // accesses[rid]->index = NULL;
        // accesses[rid]->key = -1;
    }
    sfence();
    return RCOK;
}

void txn_man::set_row_status(RC rc){
    //printf("row_cnt = %d\n", row_cnt);
    for(int rid = row_cnt - 1; rid >= 0; rid--){
        ATOM_SUB(accesses[rid]->orig_row->ref_cnt, 1);
        //printf("orig_row = %p, ref_cnt = %ld\n", accesses[rid]->orig_row, accesses[rid]->orig_row->ref_cnt);
    }
    row_cnt = 0;    
}
//cly end
