#ifndef WL_H
#define WL_H

#include <vector>
#include <unordered_map>
#include <cstdio>
#include <thread>
#include <random>
#include <mutex>
#include <signal.h>
#include <sys/wait.h>
#include "mica/util/lcore.h"
#include "mica/util/zipf.h"
#include "mica/util/rand.h"
#include "mica/transaction/db.h"
#include "mica/test/test_tx_conf.h"

#include "tuple.h"
#include "helper.h"

typedef DBConfig::Alloc Alloc;
typedef DBConfig::AepAlloc AepAlloc;
typedef DBConfig::LoggerInterface LoggerInterface;
typedef DBConfig::Logger Logger;
typedef DBConfig::AepLogger AepLogger;
typedef DBConfig::Timestamp Timestamp;
typedef DBConfig::ConcurrentTimestamp ConcurrentTimestamp;
typedef DBConfig::Timing Timing;
typedef ::mica::transaction::PagePool<DBConfig> PagePool;
typedef ::mica::transaction::DB<DBConfig> DB;
typedef ::mica::transaction::Table<DBConfig> Table;
typedef DB::HashIndexUniqueU64 HashIndex;
typedef DB::BTreeIndexUniqueU64 BTreeIndex;
typedef ::mica::transaction::RowVersion<DBConfig> RowVersion;
typedef ::mica::transaction::RowAccessHandle<DBConfig> RowAccessHandle;
typedef ::mica::transaction::RowAccessHandlePeekOnly<DBConfig>
        RowAccessHandlePeekOnly;
typedef ::mica::transaction::AepRowAccessHandle<DBConfig> AepRowAccessHandle;
typedef ::mica::transaction::AepRowAccessHandlePeekOnly<DBConfig>
        AepRowAccessHandlePeekOnly;
typedef ::mica::transaction::Transaction<DBConfig> Transaction;
typedef ::mica::transaction::AepTransaction<DBConfig> AepTransaction;
typedef ::mica::transaction::Result Result;
typedef ::mica::util::Stopwatch Stopwatch;
typedef ::mica::transaction::BTreeRangeType BTreeRangeType;

#define ALLOC_SIZE (8L<<30)
#define AEPALLOC_SIZE (32L<<30)
#define MINIMUM_METCACHE_SIZE (1L<<20)  // order/neworder/orderline表会很大，但cache不会增大
#define AEP_DRAM_RATE (4)

static Stopwatch sw;
 
struct tpcc_wl {
  tpcc_wl(void) {
    ::mica::util::lcore.pin_thread(0);
    init();
  }
  Alloc *alloc;
  AepAlloc *aepalloc;
  PagePool *pagepools[2];
  Logger *logger;
  DB *db;
  Table *t_warehouse;
  Table *t_district;
  Table *t_customer;
  Table *t_history;
  Table *t_neworder;
  Table *t_order;
  Table *t_orderline;
  Table *t_item;
  Table *t_stock;
  HashIndex *hi_warehouse;
  HashIndex *hi_district;
  HashIndex *hi_customer;
  HashIndex *hi_item;
  HashIndex *hi_stock;
  BTreeIndex *bi_order;
  BTreeIndex *bi_order_cust; 
  BTreeIndex *bi_neworder;
  BTreeIndex *bi_orderline;
  std::unordered_map<int64_t, std::vector<int64_t>> *
       hi_customer_last; // Non-primary Key, Read-only
  void init(void);
  void init_mica(void);
  void init_table_and_index(void);
  void init_data(void);
  void fill_warehouse(void);
  void fill_district(void);
  void fill_customer(void);
  void fill_item(void);
  void fill_stock(void);
};
void tpcc_wl::init(void) {
  printf("creating database ...\n");
  init_mica ();
  printf("creating table and index ...\n");
  init_table_and_index();
  printf("filling data ...\n");

  int64_t wh_thd_max = std::max(g_num_wh, g_num_thd);
  tpcc_buffer = new drand48_data*[wh_thd_max];
  for (int i = 0; i < wh_thd_max; i++) {
    tpcc_buffer[i] = (drand48_data*) new char[sizeof(drand48_data*)];
    srand48_r(i + 1, tpcc_buffer[i]);
  }
  InitNURand(0);

  init_data ();
}
void tpcc_wl::init_mica(void) {
  alloc = new Alloc(0, ALLOC_SIZE);
  aepalloc = new AepAlloc(0, AEPALLOC_SIZE);
  alloc->pre_malloc_2mb ();
  aepalloc->pre_malloc_2mb ();
  pagepools[0] = new PagePool(alloc, aepalloc, 0);  // DRAM pool
  pagepools[1] = new PagePool(alloc, aepalloc, 1);  // AEP pool
  logger = new Logger();
  logger->set_logging_thread_count(0);
  logger->disable_logging();
  sw.init_start();
  sw.init_end();
  db = new DB(pagepools, logger, &sw, g_num_thd, 0);
}
void tpcc_wl::init_table_and_index(void) {
  auto num_warehouse = g_num_wh;
  const uint64_t pnum_warehouse[] = {sizeof(tuple_warehouse)};
  db->create_table("Warehouse",1, pnum_warehouse,
      num_warehouse,std::max(MINIMUM_METCACHE_SIZE,num_warehouse/AEP_DRAM_RATE));
  auto num_district = num_warehouse*DISTRICT_PER_WAREHOUSE;
  const uint64_t pnum_district[] = {sizeof(tuple_district)};
  db->create_table("District",1,pnum_district,
      num_district,std::max(MINIMUM_METCACHE_SIZE,num_district/AEP_DRAM_RATE));
  auto num_customer = num_district*CUSTOMER_PER_DISTRICT;
  const uint64_t pnum_customer[] = {sizeof(tuple_customer)};
  db->create_table("Customer",1,pnum_customer,
      num_customer,std::max(MINIMUM_METCACHE_SIZE,num_customer/AEP_DRAM_RATE));
  auto num_history = 0L;
  const uint64_t pnum_history[] = {sizeof(tuple_history)};
  db->create_table("History",1,pnum_history,
      num_history,std::max(MINIMUM_METCACHE_SIZE,num_history/AEP_DRAM_RATE));
  auto num_neworder = 0L;    //  插入和少量热数据，并不需要很多MetCache就可以达到比较好的效果
  const uint64_t pnum_neworder[] = {sizeof(tuple_neworder)};
  db->create_table("Neworder",1,pnum_neworder,
      num_neworder,std::max(MINIMUM_METCACHE_SIZE,num_neworder/AEP_DRAM_RATE));
  auto num_order = 0L;      //  插入和少量热数据，并不需要很多MetCache就可以达到比较好的效果
  const uint64_t pnum_order[] = {sizeof(tuple_order)};
  db->create_table("Order",1,pnum_order,
      num_order,std::max(MINIMUM_METCACHE_SIZE,num_order/AEP_DRAM_RATE));
  auto num_orderline = 0L;  //  插入和少量热数据，并不需要很多MetCache就可以达到比较好的效果
  const uint64_t pnum_orderline[] = {sizeof(tuple_orderline)};
  db->create_table("Orderline",1,pnum_orderline,
      num_orderline,std::max(MINIMUM_METCACHE_SIZE,num_orderline/AEP_DRAM_RATE));
  auto num_item = ITEM_SIZE;
  const uint64_t pnum_item[] = {sizeof(tuple_item)};
  db->create_table("Item",1,pnum_item,
      num_item,std::max(MINIMUM_METCACHE_SIZE,num_item/AEP_DRAM_RATE));
  auto num_stock = num_warehouse*ITEM_SIZE; 
  const uint64_t pnum_stock[] = {sizeof(tuple_stock)};
  db->create_table("Stock",1,pnum_stock,
      num_stock,std::max(MINIMUM_METCACHE_SIZE,num_stock/AEP_DRAM_RATE));
  t_warehouse = db->get_table("Warehouse");
  t_district = db->get_table("District");
  t_customer = db->get_table("Customer");
  t_history = db->get_table("History");
  t_neworder = db->get_table("Neworder");
  t_order = db->get_table("Order");
  t_orderline = db->get_table("Orderline");
  t_item = db->get_table("Item");
  t_stock = db->get_table("Stock");
  db->create_hash_index_unique_u64 ("hi_warehouse",t_warehouse,num_warehouse);
  db->create_hash_index_unique_u64 ("hi_district",t_district,num_district);
  db->create_hash_index_unique_u64 ("hi_customer",t_customer,num_customer);
  db->create_hash_index_unique_u64 ("hi_item",t_item,num_item);
  db->create_hash_index_unique_u64 ("hi_stock",t_stock,num_stock);
  db->create_btree_index_unique_u64("bi_order",t_order);
  db->create_btree_index_unique_u64("bi_neworder",t_neworder);
  db->create_btree_index_unique_u64("bi_order_cust",t_order);
  db->create_btree_index_unique_u64("bi_orderline",t_orderline);
  hi_warehouse = db->get_hash_index_unique_u64("hi_warehouse");
  hi_district = db->get_hash_index_unique_u64("hi_district");
  hi_customer = db->get_hash_index_unique_u64("hi_customer");
  hi_customer_last = new std::unordered_map<int64_t, std::vector<int64_t>>;
  hi_item = db->get_hash_index_unique_u64("hi_item");
  hi_stock = db->get_hash_index_unique_u64("hi_stock");
  bi_order = db->get_btree_index_unique_u64("bi_order");
  bi_neworder = db->get_btree_index_unique_u64("bi_neworder");
  bi_order_cust = db->get_btree_index_unique_u64("bi_order_cust"); 
  bi_orderline = db->get_btree_index_unique_u64("bi_orderline");
  db->activate(0);
  Transaction tx(db->context(0));
  hi_warehouse->init(&tx);
  hi_district->init(&tx);
  hi_customer->init(&tx);
  hi_item->init(&tx);
  hi_stock->init(&tx);
  bi_order->init(&tx);
  bi_neworder->init(&tx);
  bi_order_cust->init(&tx); 
  bi_orderline->init(&tx);
  db->deactivate(0);
}
void tpcc_wl::init_data(void) {
  // 数据相关
  db->activate(0);
  fill_warehouse();
  fill_district();
  fill_customer();
  fill_item();
  fill_stock();
  db->deactivate(0);
}
void tpcc_wl::fill_warehouse(void) {
  const int64_t batch = 16;
  AepTransaction tx(db->context(0));
  bool res = false;
  Result cmt;
  auto &tbl = t_warehouse;
  auto dsz = sizeof(tuple_warehouse);
  for (int64_t i=0; i<g_num_wh; i+=batch) {
    auto j = std::min(i+batch, g_num_wh);
    res = tx.aep_begin();
    assert(res == true);
    for (auto k=i; k<j; k++) {
      AepRowAccessHandle rah(&tx);
      res = rah.aep_new_row(tbl,0,Transaction::kNewRowID,k+1,true,dsz);
      assert(res == true);
      auto tp = (tuple_warehouse *)rah.data();
      tp->w_id = k+1;
      snprintf(tp->w_name, 10, "W-%ld", tp->w_id);
      tp->w_tax = 0.01;
      tp->w_ytd = 0.0;
      auto cnt = hi_warehouse->insert(&tx,tp->w_id,rah.row_id());
      assert (cnt == 1);
    }
    res = tx.aep_commit(&cmt);
    assert(res == true);
  }
}
void tpcc_wl::fill_district(void) {
  const int64_t batch = 16;
  AepTransaction tx(db->context(0));
  bool res = false;
  Result cmt;
  auto &tbl = t_district;
  auto dsz = sizeof(tuple_district);
  for (int64_t i=0; i<g_num_wh*DISTRICT_PER_WAREHOUSE; i+=batch) {
    auto j = std::min(i+batch, g_num_wh*DISTRICT_PER_WAREHOUSE);
    res = tx.aep_begin();
    assert(res == true);
    for (auto k=i; k<j; k++) {
      AepRowAccessHandle rah(&tx);
      auto dk = distKey(k%DISTRICT_PER_WAREHOUSE+1,k/DISTRICT_PER_WAREHOUSE+1);
      res = rah.aep_new_row(tbl,0,Transaction::kNewRowID,dk,true,dsz);
      assert(res == true);
      auto tp = (tuple_district *)rah.data();
      tp->d_id = k%DISTRICT_PER_WAREHOUSE+1;
      tp->d_w_id = k/DISTRICT_PER_WAREHOUSE+1;
      snprintf(tp->d_name, 10, "D-%ld", dk);
      tp->d_tax = 0.02;
      tp->d_ytd = 0.0;
      tp->d_next_o_id = 1;
      auto cnt = hi_district->insert(&tx,dk,rah.row_id());
      assert (cnt == 1);
    }
    res = tx.aep_commit(&cmt);
    assert(res == true);
  }
}
void tpcc_wl::fill_customer(void) {
  const int64_t batch = 16;
  AepTransaction tx(db->context(0));
  bool res = false;
  Result cmt;
  auto &tbl = t_customer;
  auto dsz = sizeof(tuple_customer);
  for (int64_t i=0; i<g_num_wh*DISTRICT_PER_WAREHOUSE*CUSTOMER_PER_DISTRICT; i+=batch) {
    auto j = std::min(i+batch, g_num_wh*DISTRICT_PER_WAREHOUSE*CUSTOMER_PER_DISTRICT);
    res = tx.aep_begin();
    assert(res == true);
    for (auto k=i; k<j; k++) {
      AepRowAccessHandle rah(&tx);

      auto ck = custKey(k%CUSTOMER_PER_DISTRICT+1,
                        k/CUSTOMER_PER_DISTRICT%DISTRICT_PER_WAREHOUSE+1,
                        k/CUSTOMER_PER_DISTRICT/DISTRICT_PER_WAREHOUSE+1);
 
      res = rah.aep_new_row(tbl,0,Transaction::kNewRowID,ck,true,dsz);
      assert(res == true);
      auto tp = (tuple_customer *)rah.data();
      tp->c_id = k%CUSTOMER_PER_DISTRICT+1;
      tp->c_d_id = k/CUSTOMER_PER_DISTRICT%DISTRICT_PER_WAREHOUSE+1;
      tp->c_w_id = k/CUSTOMER_PER_DISTRICT/DISTRICT_PER_WAREHOUSE+1;

      MakeAlphaString(6, 16, tp->c_first, tp->c_w_id - 1);
      if (tp->c_id <= 1000) Lastname(tp->c_id - 1, tp->c_last);
      else Lastname(NURand(255, 0, 999, tp->c_w_id - 1), tp->c_last);

      if (RAND(10, tp->c_w_id - 1) == 0) strncpy(tp->c_credit, "GC", 2);
      else strncpy(tp->c_credit, "BC", 2);
 
      tp->c_credit_lim = 50000.0;
      tp->c_since = 1000;
      tp->c_discount = (double)URand(1, 5000, tp->c_w_id - 1) / 10000.0;
      tp->c_balance = -10.0;
      tp->c_ytd_payment = 10.0;
      tp->c_payment_cnt = 0;
      tp->c_delivery_cnt = 0;

      auto cnt = hi_customer->insert(&tx, ck ,rah.row_id());
      assert (cnt == 1);

      // 插入Lastname非主键只读索引
      int64_t npk = custNPKey(tp->c_d_id, tp->c_w_id, tp->c_last);
      (*hi_customer_last)[npk].push_back(ck);
    }
    res = tx.aep_commit(&cmt);
    assert(res == true);
  }
}
void tpcc_wl::fill_item(void) {
  const int64_t batch = 16;
  AepTransaction tx(db->context(0));
  bool res = false;
  Result cmt;
  auto &tbl = t_item;
  auto dsz = sizeof(tuple_item);
  int cnt; 
  for (int64_t i=0; i<ITEM_SIZE; i+=batch) {
    auto j = std::min(i+batch, ITEM_SIZE);
    res = tx.aep_begin();
    assert(res == true);
    for (auto k=i; k<j; k++) {
      AepRowAccessHandle rah(&tx);
      res = rah.aep_new_row(tbl,0,Transaction::kNewRowID,k+1,true,dsz);
      assert(res == true);
      auto tp = (tuple_item *)rah.data();
      tp->i_id = k+1;
      snprintf(tp->i_name, 24, "I-%ld", k);
      tp->i_price = k;
      auto cnt = hi_item->insert(&tx,k+1,rah.row_id());
      assert (cnt == 1);
    }
    res = tx.aep_commit(&cmt);
    assert(res == true);
  }
}
void tpcc_wl::fill_stock(void) {
  const int64_t batch = 16;
  AepTransaction tx(db->context(0));
  bool res = false;
  Result cmt;
  auto &tbl = t_stock;
  auto dsz = sizeof(tuple_stock);
  for (int64_t i=0; i<g_num_wh*ITEM_SIZE; i+=batch) {
    auto j = std::min(i+batch, g_num_wh*ITEM_SIZE);
    res = tx.aep_begin();
    assert(res == true);
    for (auto k=i; k<j; k++) {
      AepRowAccessHandle rah(&tx);
      auto sk = stockKey(k%ITEM_SIZE+1, k/ITEM_SIZE+1);
      res = rah.aep_new_row(tbl,0,Transaction::kNewRowID,sk,true,dsz);
      assert(res == true);
      auto tp = (tuple_stock *)rah.data();
      tp->s_i_id = k%ITEM_SIZE+1;
      tp->s_w_id = k/ITEM_SIZE+1;
      tp->s_quantity = URand(10, 100, tp->s_w_id-1);
      MakeAlphaString(24, 24, tp->s_dist_01, 0);
      MakeAlphaString(24, 24, tp->s_dist_02, 0);
      MakeAlphaString(24, 24, tp->s_dist_03, 0);
      MakeAlphaString(24, 24, tp->s_dist_04, 0);
      MakeAlphaString(24, 24, tp->s_dist_05, 0);
      MakeAlphaString(24, 24, tp->s_dist_06, 0);
      MakeAlphaString(24, 24, tp->s_dist_07, 0);
      MakeAlphaString(24, 24, tp->s_dist_08, 0);
      MakeAlphaString(24, 24, tp->s_dist_09, 0);
      MakeAlphaString(24, 24, tp->s_dist_10, 0);
      tp->s_ytd = 0.0;
      tp->s_order_cnt = 0;
      tp->s_remote_cnt = 0;
      auto cnt = hi_stock->insert(&tx,sk,rah.row_id());
      assert (cnt == 1);
    }
    res = tx.aep_commit(&cmt);
    assert(res == true);
  }
}
#endif
