#ifndef QUERY_H
#define QUERY_H

#include <stdlib.h>
#include "helper.h"

enum TPCCTxnType {
TPCC_PAYMENT=0,TPCC_NEW_ORDER,TPCC_ORDER_STATUS,TPCC_DELIVERY,TPCC_STOCK_LEVEL};

struct tpcc_query_payment {  // 以主键标识的客户付款给特定仓库的特定街区，为某个订单
  int64_t w_id;
  int64_t d_id;
  int64_t c_w_id;
  int64_t c_d_id;
  double h_amount;
  int64_t h_date;
  bool by_last_name;
  int64_t c_id;      // by_last_name == true
  char c_last[16];   // by_last_name == false
};
struct tpcc_query_new_order {  // 以主键为标识的客户购买1-15件：任意仓库的库存商品
  int64_t w_id;
  int64_t d_id;
  int64_t c_id;
  int64_t ol_cnt;
  int64_t o_entry_d;
  struct Item_no *item;
  bool all_local;
};
struct Item_no {
  int64_t ol_i_id;
  int64_t ol_supply_w_id;
  int64_t ol_quantity;
};
struct tpcc_query_order_status {  // 查询某一特定街区的客户的最后一个订单信息
  int64_t w_id;
  int64_t d_id;
  bool by_last_name;
  int64_t c_id;     // by_last_name == true
  char c_last[16];  // by_last_name == false
};
struct tpcc_query_stock_level {  // 查询某个街区是否足够供应当前已接收的订单
  int64_t w_id;
  int64_t d_id;
  int64_t threshold;
};
struct tpcc_query_delivery {  // 仓库发货将货物送达
  int64_t w_id;
  int64_t o_carrier_id;
  int64_t ol_delivery_d;
};
struct tpcc_query {
  TPCCTxnType type;
  union {
    tpcc_query_payment payment;
    tpcc_query_new_order new_order;
    tpcc_query_order_status order_status;
    tpcc_query_stock_level stock_level;
    tpcc_query_delivery delivery;
  } args;
  void init(int64_t thd_id);
  void gen_payment(int64_t thd_id);
  void gen_new_order(int64_t thd_id);
  void gen_order_status(int64_t thd_id);
  void gen_stock_level(int64_t thd_id);
  void gen_delivery(int64_t thd_id);
};
Item_no *items[MAXIMUM_THREAD];
int64_t cur_item[MAXIMUM_THREAD];
int64_t num_gen_query[MAXIMUM_THREAD][TPCC_TXN_TYPES];
struct tpcc_thd {
  int64_t thd_id;
  void init (int64_t thd_id) {
    cur_item[thd_id] = 0;
    items[thd_id] = new Item_no[g_num_que*MAXIMUM_ITEM_PER_NEWORDER];
    queries = new tpcc_query[g_num_que];
    for (int64_t i=0; i<g_num_que; i++) {
      queries[i].init(thd_id);
    }
  }
  tpcc_query *queries;
};
struct tpcc_thds {
  tpcc_thd *thds;
  tpcc_thds(void) {
    int64_t wh_thd_max = std::max(g_num_wh, g_num_thd);
    tpcc_buffer = new drand48_data*[wh_thd_max];
    for (int i = 0; i < wh_thd_max; i++) {
      tpcc_buffer[i] = (drand48_data*) new char[sizeof(drand48_data*)];
      srand48_r(i + 1, tpcc_buffer[i]);
    }
    InitNURand(0);
    thds = new tpcc_thd[g_num_thd];
    for (int64_t i=0; i<g_num_thd; i++) {
      thds[i].init(i);
    }
  }
  tpcc_query* get_queries(int64_t thd_id) { return thds[thd_id].queries; }
};
void tpcc_query::init(int64_t thd_id) {
  double x = (double)URand(0, 99, thd_id) / 100.0;
  if (x < 0.04)
    gen_stock_level(thd_id);
  else if (x < 0.04 + 0.04)
    gen_delivery(thd_id);
  else if (x < 0.04 + 0.04 + 0.04)
    gen_order_status(thd_id);
  else if (x < 0.04 + 0.04 + 0.04 + 0.43)
    gen_payment(thd_id);
  else
    gen_new_order(thd_id);
}
void tpcc_query::gen_payment(int64_t thd_id) {
  type = TPCC_PAYMENT;
  num_gen_query[thd_id][type] += 1;
  tpcc_query_payment& arg = args.payment;
  arg.w_id = URand(1, g_num_wh, thd_id);
  arg.d_id = URand(1, DISTRICT_PER_WAREHOUSE, thd_id);
  arg.h_amount = URand(1, 5000, thd_id);
  arg.h_date = 2013;
  int x = URand(1, 100, thd_id);
  int y = URand(1, 100, thd_id);
  if (x <= 85) {
    // home warehouse
    arg.c_d_id = arg.d_id;
    arg.c_w_id = arg.w_id;
  } else {
    // remote warehouse
    arg.c_d_id = URand(1, DISTRICT_PER_WAREHOUSE, thd_id);
    if (g_num_wh > 1) {
      while ((arg.c_w_id = URand(1, g_num_wh, thd_id)) == arg.w_id) {
      }
    } else
      arg.c_w_id = arg.w_id;
  }
  if (y <= 60) {
    arg.by_last_name = true;
    Lastname(NURand(255, 0, 999, thd_id), arg.c_last);
  } else {
    arg.by_last_name = false;
    arg.c_id = NURand(1023, 1, CUSTOMER_PER_DISTRICT, thd_id);
  }
}
void tpcc_query::gen_new_order(int64_t thd_id) {
  type = TPCC_NEW_ORDER;
  num_gen_query[thd_id][type] += 1;
  tpcc_query_new_order& arg = args.new_order;
  arg.w_id = URand(1, g_num_wh, thd_id);
  arg.d_id = URand(1, DISTRICT_PER_WAREHOUSE, thd_id);
  arg.c_id = NURand(1023, 1, CUSTOMER_PER_DISTRICT, thd_id);
  arg.ol_cnt = URand(5, 15, thd_id);
  arg.o_entry_d = 2013;
  arg.all_local = true;
  arg.item = items[thd_id]+cur_item[thd_id];
  cur_item[thd_id] += arg.ol_cnt;
  for (int32_t oid = 0; oid < arg.ol_cnt; oid++) {
    arg.item[oid].ol_i_id = NURand(8191, 1, ITEM_SIZE, thd_id);
    int32_t x = URand(1, 100, thd_id);
    if (x > 1 || g_num_wh == 1)
      arg.item[oid].ol_supply_w_id = arg.w_id;
    else {
      while ((arg.item[oid].ol_supply_w_id = URand(1, g_num_wh, thd_id)) ==
             arg.w_id) {
      }
      arg.all_local = false;
    }
    arg.item[oid].ol_quantity = URand(1, 10, thd_id);
  }
  // Remove duplicate item
  for (int32_t i = 0; i < arg.ol_cnt; i++) {
    for (int32_t j = 0; j < i; j++) {
      if (arg.item[i].ol_i_id == arg.item[j].ol_i_id) {
        for (int32_t k = i; k < arg.ol_cnt - 1; k++)
          arg.item[k] = arg.item[k + 1];
        arg.ol_cnt--;
        i--;
      }
    }
  }
  for (int32_t i = 0; i < arg.ol_cnt; i++)
    for (int32_t j = 0; j < i; j++)
      assert(arg.item[i].ol_i_id != arg.item[j].ol_i_id);
  // "1% of new order gives wrong itemid"
  if (URand(0, 99, thd_id) == 0) {
    arg.item[URand(0, arg.ol_cnt - 1, thd_id)].ol_i_id = -1;
  }
}
void tpcc_query::gen_order_status(int64_t thd_id) {
  type = TPCC_ORDER_STATUS;
  num_gen_query[thd_id][type] += 1;
  tpcc_query_order_status& arg = args.order_status;
  arg.w_id = URand(1, g_num_wh, thd_id);
  arg.d_id = URand(1, DISTRICT_PER_WAREHOUSE, thd_id);
  int y = URand(1, 100, thd_id);
  if (y <= 60) {
    arg.by_last_name = true;
    Lastname(NURand(255, 0, 999, thd_id), arg.c_last);
  } else {
    arg.by_last_name = false;
    arg.c_id = NURand(1023, 1, CUSTOMER_PER_DISTRICT, thd_id);
  }
}
void tpcc_query::gen_stock_level(int64_t thd_id) {
  type = TPCC_STOCK_LEVEL;
  num_gen_query[thd_id][type] += 1;
  tpcc_query_stock_level& arg = args.stock_level;
  arg.w_id = URand(1, g_num_wh, thd_id);
  arg.d_id = URand(1, DISTRICT_PER_WAREHOUSE, thd_id);
  arg.threshold = URand(10, 20, thd_id);
}
void tpcc_query::gen_delivery(int64_t thd_id) {
  type = TPCC_DELIVERY;
  num_gen_query[thd_id][type] += 1;
  tpcc_query_delivery& arg = args.delivery;
  arg.w_id = URand(1, g_num_wh, thd_id);
  arg.o_carrier_id = URand(1, DISTRICT_PER_WAREHOUSE, thd_id);
  arg.ol_delivery_d = 2013;
}
#endif
