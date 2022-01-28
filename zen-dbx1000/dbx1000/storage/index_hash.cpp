#include "global.h"	
#include "index_hash.h"
#include "mem_alloc.h"
#include "table.h"

RC IndexHash::init(uint64_t bucket_cnt, int part_cnt) {
	_bucket_cnt = bucket_cnt;
	_bucket_cnt_per_part = bucket_cnt / part_cnt;
	_buckets = new BucketHeader * [part_cnt];
	for (int i = 0; i < part_cnt; i++) {
		_buckets[i] = (BucketHeader *) _mm_malloc(sizeof(BucketHeader) * _bucket_cnt_per_part, 64);
		for (uint32_t n = 0; n < _bucket_cnt_per_part; n ++)
			_buckets[i][n].init();
	}
	return RCOK;
}

RC 
IndexHash::init(int part_cnt, table_t * table, uint64_t bucket_cnt) {
	init(bucket_cnt, part_cnt);
	this->table = table;
	return RCOK;
}

bool IndexHash::index_exist(idx_key_t key) {
	assert(false);
}

void 
IndexHash::get_latch(BucketHeader * bucket) {
	while (!ATOM_CAS(bucket->locked, false, true)) {}
}

void 
IndexHash::release_latch(BucketHeader * bucket) {
	bool ok = ATOM_CAS(bucket->locked, true, false);
	assert(ok);
}

	
RC IndexHash::index_insert(idx_key_t key, itemid_t * item, int part_id) {
    
	RC rc = RCOK;
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	// 1. get the ex latch
	get_latch(cur_bkt);
	
	// 2. update the latch list
	cur_bkt->insert_item(key, item, part_id);
	
	// 3. release the latch
	release_latch(cur_bkt);
	return rc;
}

RC IndexHash::index_read(idx_key_t key, itemid_t * &item, int part_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
	//get_latch(cur_bkt);
	cur_bkt->read_item(key, item, table->get_table_name());
	// 3. release the latch
	//release_latch(cur_bkt);
	return rc;

}

RC IndexHash::index_read(idx_key_t key, itemid_t * &item, 
						int part_id, int thd_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
	//get_latch(cur_bkt);
	cur_bkt->read_item(key, item, table->get_table_name());
	// 3. release the latch
	//release_latch(cur_bkt);
	return rc;
}

// cly begin
RC IndexHash::index_remove(idx_key_t key, int part_id){
    uint64_t bkt_idx = hash(key);
    assert(bkt_idx < _bucket_cnt_per_part);
    BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
    RC rc = RCOK;
    get_latch(cur_bkt);
    cur_bkt->remove_items(key);
    release_latch(cur_bkt);
    return rc;
}

RC IndexHash::index_replace(idx_key_t key, itemid_t * item, int part_id){
    uint64_t bkt_idx = hash(key);
    assert(bkt_idx < _bucket_cnt_per_part);
    BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
    RC rc = RCOK;
    get_latch(cur_bkt);
    cur_bkt->remove_items(key);
    cur_bkt->insert_item(key, item, part_id);
    release_latch(cur_bkt);
    return rc;
}

RC IndexHash::index_get_latch(idx_key_t key, int part_id){
    uint64_t bkt_idx = hash(key);
    assert(bkt_idx < _bucket_cnt_per_part);
    BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
    RC rc = RCOK;
    get_latch(cur_bkt);
    return rc;
}

RC IndexHash::index_release_latch(idx_key_t key, int part_id){
    uint64_t bkt_idx = hash(key);
    assert(bkt_idx < _bucket_cnt_per_part);
    BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
    RC rc = RCOK;
    release_latch(cur_bkt);
    return rc;
}

RC IndexHash::index_replace_without_getting_latch(idx_key_t key, itemid_t * item, int part_id){
    uint64_t bkt_idx = hash(key);
    assert(bkt_idx < _bucket_cnt_per_part);
    BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
    RC rc = RCOK;
    //get_latch(cur_bkt);
    cur_bkt->remove_items(key);
    cur_bkt->insert_item(key, item, part_id);
    //release_latch(cur_bkt);
    return rc;
}
// cly end

/************** BucketHeader Operations ******************/

void BucketHeader::init() {
	node_cnt = 0;
	first_node = NULL;
	locked = false;
}

void BucketHeader::insert_item(idx_key_t key, 
		itemid_t * item, 
		int part_id) 
{
	BucketNode * cur_node = first_node;
	BucketNode * prev_node = NULL;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		prev_node = cur_node;
		cur_node = cur_node->next;
	}
	if (cur_node == NULL) {		
		BucketNode * new_node = (BucketNode *) 
			mem_allocator.alloc(sizeof(BucketNode), part_id );
		new_node->init(key);
		new_node->items = item;
		if (prev_node != NULL) {
			new_node->next = prev_node->next;
			prev_node->next = new_node;
		} else {
			new_node->next = first_node;
			first_node = new_node;
		}
	} else {
		item->next = cur_node->items;
		cur_node->items = item;
	}
}

void BucketHeader::read_item(idx_key_t key, itemid_t * &item, const char * tname) 
{
	BucketNode * cur_node = first_node;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		cur_node = cur_node->next;
	}
    //cly begin
    /*if(cur_node == NULL){
        printf("cur_node == NULL");
        return;
    }*/
    M_ASSERT(cur_node != NULL, "cur_node doesn't exist!");
    //cly end
	M_ASSERT(cur_node->key == key, "Key does not exist!");
	item = cur_node->items;
}

// cly begin
void BucketHeader::remove_items(idx_key_t key){
    BucketNode * prev_node = NULL;
    BucketNode * cur_node = first_node;
    while (cur_node != NULL) {
        if(cur_node->key == key)
            break;
        prev_node = cur_node;
        cur_node = cur_node->next;
    }
    if(cur_node == NULL)
        return;
    M_ASSERT(cur_node->key == key, "Key to remove does not exist!");

    // TODO it seems like one key can relate to more than one item, which is not reasonable! 
    // the implementation now deletes all of the items related to one key
    if(prev_node == NULL) // cur_node is the first node in this bucket
        first_node = cur_node->next;
    else
        prev_node->next = cur_node->next;

    // free all of the items
    itemid_t * cur_item = cur_node->items;
    itemid_t * next_item = NULL;
    while(cur_item != NULL){
        assert(cur_item);
        next_item = cur_item->next;
        mem_allocator.free(cur_item, sizeof(itemid_t));
        cur_item = next_item;
    }
    assert(cur_node);
    //return cur_node;
    //mem_allocator.free(cur_node, sizeof(BucketNode));    
}
// cly end
