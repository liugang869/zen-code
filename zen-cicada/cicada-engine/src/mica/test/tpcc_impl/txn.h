#ifndef TXN_H
#define TXN_H

#include "wl.h"
#include "query.h"
#include <unordered_map>

enum RC {RCOK, Commit, Abort, WAIT, ERROR, FINISH};
int64_t committed_query[MAXIMUM_THREAD][TPCC_TXN_TYPES];

tpcc_wl *g_wl = nullptr;
tpcc_thds *g_thds = nullptr;

int64_t g_history[MAXIMUM_THREAD];   //  线程插入History表的数量

class tpcc_txn_man {
 private:
  int64_t thdid;
  AepTransaction *tx;

 public:
  tpcc_txn_man (int64_t thdid, AepTransaction *tx) { 
    this->thdid = thdid;
    this->tx = tx;
  }
  RC run_txn(tpcc_query* query);
  RC run_payment(tpcc_query* query);
  RC run_new_order(tpcc_query* query);
  RC run_order_status(tpcc_query* query);
  RC run_delivery(tpcc_query* query);
  RC run_stock_level(tpcc_query* query);
};

RC tpcc_txn_man::run_txn(tpcc_query* query) {
  RC rc = ERROR;
  switch (query->type) {
    case TPCC_PAYMENT:      rc = run_payment(query); break;
    case TPCC_NEW_ORDER:    rc = run_new_order(query); break;
    case TPCC_ORDER_STATUS: rc = run_order_status(query); break;
    case TPCC_DELIVERY:     rc = run_delivery(query); break;
    case TPCC_STOCK_LEVEL:  rc = run_stock_level(query); break;
    default:                rc = ERROR; assert(false);
  }
  return rc;
}
RC tpcc_txn_man::run_payment(tpcc_query* query) {
  auto& arg = query->args.payment;
  tx->aep_begin(false);  // 是否是只读事务

  int64_t row_id;
  uint64_t lookup_result;

  // 1.更新warehouse表
  row_id = warehouseKey(arg.w_id);
  lookup_result =
  g_wl->hi_warehouse->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
  if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
    // M_ASSERT (false, "hi_warehouse: not found!\n");
    tx->aep_abort(); return Abort;
  }
  AepRowAccessHandle rahw(tx); 
  if (!rahw.aep_peek_row(g_wl->t_warehouse, 0, row_id, false, true, true) ||
    !rahw.aep_read_row() || !rahw.aep_write_row(sizeof(tuple_warehouse))) {
    // M_ASSERT (false, "hi_warehouse: rah not found!\n");
    tx->aep_abort(); return Abort;
  }
  tuple_warehouse *tw = (tuple_warehouse*)rahw.data();
  tw->w_ytd += arg.h_amount;

  // 2.更新district表
  row_id = distKey(arg.d_id, arg.w_id);
  lookup_result =
  g_wl->hi_district->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
  if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
    // M_ASSERT (false, "hi_district: not found!\n");
    tx->aep_abort(); return Abort;
  }
  AepRowAccessHandle rahd(tx); 
  if (!rahd.aep_peek_row(g_wl->t_district, 0, row_id, false, true, true) ||
    !rahd.aep_read_row() || !rahd.aep_write_row(sizeof(tuple_district))) {
    // M_ASSERT (false, "hi_district: rah not found!\n");
    tx->aep_abort(); return Abort;
  }
  tuple_district *td = (tuple_district*)rahd.data();
  td->d_ytd += arg.h_amount;

  // 3.更新customer表
  tuple_customer *tc;
  if (!arg.by_last_name) {
    row_id = custKey(arg.c_id, arg.d_id, arg.w_id);
  }
  else {
    row_id = custNPKey(arg.d_id, arg.w_id, arg.c_last);
    auto i = g_wl->hi_customer_last->find(row_id);  // lastname输入总是正确的
    auto &v = i->second;
    row_id = v[v.size()/2];
  }
  lookup_result =
  g_wl->hi_customer->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
  if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
    // M_ASSERT (false, "hi_customer: not found!\n");
    tx->aep_abort(); return Abort;
  }
  AepRowAccessHandle rahc(tx); 
  if (!rahc.aep_peek_row(g_wl->t_customer, 0, row_id, false, true, true) ||
    !rahc.aep_read_row() || !rahc.aep_write_row(sizeof(tuple_customer))) {
    // M_ASSERT (false, "hi_customer: rah not found!\n");
    tx->aep_abort(); return Abort;
  }
  tc = (tuple_customer*)rahc.data();
  tc->c_balance -= arg.h_amount;
  tc->c_ytd_payment += arg.h_amount;
  tc->c_payment_cnt += 1;
  if (strncmp(tc->c_credit, "BC", 2)==0) {
    char c_new_data[501];
    int dlen = sprintf(c_new_data, "%4ld %2ld %4ld %2ld %4ld $%7.2f | ", tc->c_id, tc->c_d_id,
               tc->c_w_id, td->d_id, tw->w_id, arg.h_amount);
    strncat(c_new_data, tc->c_data, 500 - dlen);
    strncpy(tc->c_data, c_new_data, 500);
  }
 
  // 4.插入history表
  AepRowAccessHandle rahh(tx); 
  rahh.aep_new_row(g_wl->t_history, 0, Transaction::kNewRowID, 
                   thdid*MAXIMUM_HISTORY+g_history[thdid], true, sizeof(tuple_history));
  g_history[thdid] += 1;
  tuple_history *th = (tuple_history*)rahh.data();
  th->h_c_id = tc->c_id;
  th->h_c_d_id = tc->c_d_id;
  th->h_c_w_id = tc->c_w_id;
  th->h_d_id = td->d_id;
  th->h_w_id = tw->w_id;
  th->h_date = 2023;
  th->h_amount = arg.h_amount;

  return tx->aep_commit() == true? Commit: Abort;
}
RC tpcc_txn_man::run_new_order(tpcc_query* query) {
  auto& arg = query->args.new_order;
  tx->aep_begin(false);  // 是否是只读事务

  int64_t row_id;
  uint64_t lookup_result;
  int64_t inscnt;

  // 1.读取item信息，判断query中item是否有效
  tuple_item *tis[15];
  for (int64_t i=1; i<= arg.ol_cnt; i++) {
    row_id = itemKey(arg.item[i-1].ol_i_id);
    lookup_result =
    g_wl->hi_item->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
    if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
      // M_ASSERT (false, "hi_item: not found!\n");
      tx->aep_abort(); return Abort;   // 1%用户输入错误在这里abort
    }
    AepRowAccessHandle rahi(tx); 
    if (!rahi.aep_peek_row(g_wl->t_item, 0, row_id, false, true, false) ||
      !rahi.aep_read_row()) {
      // M_ASSERT (false, "hi_item: rah not found!\n");
      tx->aep_abort(); return Abort;
    }
    tis[i-1] = (tuple_item*)rahi.cdata();
  }

  // 2.读取warehouse的税率
  row_id = warehouseKey(arg.w_id);
  lookup_result =
  g_wl->hi_warehouse->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
  if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
    // M_ASSERT (false, "hi_item: not found!\n");
    tx->aep_abort(); return Abort;   // 1%用户输入错误在这里abort
  }
  AepRowAccessHandle rahw(tx); 
  if (!rahw.aep_peek_row(g_wl->t_item, 0, row_id, false, true, false) ||
    !rahw.aep_read_row()) {
    // M_ASSERT (false, "hi_warehouse: rah not found!\n");
    tx->aep_abort(); return Abort;
  }
  tuple_warehouse *tw = (tuple_warehouse*)rahw.cdata();
  double w_tax = tw->w_tax;
  
  // 3.读取district税率和更新d_next_o_id
  row_id = distKey(arg.d_id, arg.w_id);
  lookup_result =
  g_wl->hi_district->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
  if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
    // M_ASSERT (false, "hi_district: not found!\n");
    tx->aep_abort(); return Abort;
  }
  AepRowAccessHandle rahd(tx); 
  if (!rahd.aep_peek_row(g_wl->t_district, 0, row_id, false, true, true) ||
    !rahd.aep_read_row() || !rahd.aep_write_row(sizeof(tuple_district))) {
    // M_ASSERT (false, "hi_district: rah not found!\n");
    tx->aep_abort(); return Abort;
  }
  tuple_district *td = (tuple_district*)rahd.data();
  double d_tax = td->d_tax;
  int64_t o_id = td->d_next_o_id++;

  // 4.读取customer折扣比例
  row_id = custKey(arg.c_id, arg.d_id, arg.w_id);
  lookup_result =
  g_wl->hi_customer->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
  if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
    // M_ASSERT (false, "hi_customer: not found!\n");
    tx->aep_abort(); return Abort;
  }
  AepRowAccessHandle rahc(tx); 
  if (!rahc.aep_peek_row(g_wl->t_customer, 0, row_id, false, true, false) ||
    !rahc.aep_read_row()) {
    // M_ASSERT (false, "hi_customer: rah not found!\n");
    tx->aep_abort(); return Abort;
  }
  tuple_customer *tc = (tuple_customer*)rahc.cdata();
  double c_discount = tc->c_discount;
   
  // 6.插入order表
  int64_t o_carrier_id = 0; // 默认值，未delivery，相当于空值标记
  AepRowAccessHandle raho(tx); 
  raho.aep_new_row(g_wl->t_order, 0, Transaction::kNewRowID, 
    orderKey(o_id, arg.d_id, arg.w_id), true, sizeof(tuple_order));
  tuple_order *to = (tuple_order*)raho.data();
  to->o_id = o_id;
  to->o_d_id = arg.d_id;
  to->o_w_id = arg.w_id;
  to->o_c_id = arg.c_id;
  to->o_entry_d = arg.o_entry_d;
  to->o_carrier_id = o_carrier_id;
  to->o_ol_cnt = arg.ol_cnt;
  to->o_all_local = arg.all_local;
  auto ok = orderKey(to->o_id, to->o_d_id, to->o_w_id);
  inscnt = g_wl->bi_order->insert(tx, ok ,raho.row_id());
  if (inscnt != 1 || inscnt == BTreeIndex::kHaveToAbort) {
    // M_ASSERT (false, "bi_order: fail insert!\n");
    tx->aep_abort(); return Abort;
  }
  auto ock = orderCustKey(to->o_id, to->o_c_id, to->o_d_id, to->o_w_id);
  inscnt = g_wl->bi_order_cust->insert(tx, ock ,raho.row_id());
  if (inscnt != 1 || inscnt == BTreeIndex::kHaveToAbort) {
    // M_ASSERT (false, "bi_neworder_cust: fail insert!\n");
    tx->aep_abort(); return Abort;
  }

  // 7.插入neworder表
  AepRowAccessHandle rahn(tx); 
  rahn.aep_new_row(g_wl->t_neworder, 0, Transaction::kNewRowID, 
                  neworderKey(o_id, arg.d_id, arg.w_id), true, sizeof(tuple_neworder));
  tuple_neworder *tn = (tuple_neworder*)rahn.data();
  tn->no_o_id = o_id;
  tn->no_d_id = arg.d_id;
  tn->no_w_id = arg.w_id;
  auto nk = neworderKey(tn->no_o_id, tn->no_d_id, tn->no_w_id);
  inscnt = g_wl->bi_neworder->insert(tx, nk ,raho.row_id());
  if (inscnt != 1 || inscnt == BTreeIndex::kHaveToAbort) {
    // M_ASSERT (false, "bi_neworder: fail insert!\n");
    tx->aep_abort(); return Abort;
  }

  // 8.更新库存，插入orderline表
  for (int64_t i=1; i<=arg.ol_cnt; i++) {
    auto ol_i_id = arg.item[i-1].ol_i_id;
    auto ol_supply_w_id = arg.item[i-1].ol_supply_w_id;
    auto ol_quantity = arg.item[i-1].ol_quantity;

    // 更新库存
    row_id = stockKey(ol_i_id, ol_supply_w_id);
    lookup_result =
    g_wl->hi_stock->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
    if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
      // M_ASSERT (false, "hi_stock: not found!\n");
      tx->aep_abort(); return Abort;
    }
    AepRowAccessHandle rahs(tx); 
    if (!rahs.aep_peek_row(g_wl->t_stock, 0, row_id, false, true, true) ||
      !rahs.aep_read_row() || !rahs.aep_write_row(sizeof(tuple_stock))) {
      // M_ASSERT (false, "hi_stock: rah not found!\n");
      tx->aep_abort(); return Abort;
    }
    tuple_stock *ts = (tuple_stock*)rahs.data();
    bool remote = ol_supply_w_id != arg.w_id;
    ts->s_ytd += ol_quantity;
    ts->s_order_cnt += 1;
    if (remote) { ts->s_remote_cnt += 1; }
    if(ts->s_quantity > ol_quantity+10) { ts->s_quantity -= ol_quantity; }
    else { ts->s_quantity += (91-ol_quantity); }

    // 插入orderline
    double i_price = tis[i-1]->i_price;
    double ol_amount = ol_quantity * i_price;
    AepRowAccessHandle rahol(tx); 
    rahol.aep_new_row(g_wl->t_orderline, 0, Transaction::kNewRowID, 
      orderlineKey(i, o_id, arg.d_id, arg.w_id),true,sizeof(tuple_orderline));
    tuple_orderline *tol = (tuple_orderline*)rahol.data();
    tol->ol_o_id = o_id;
    tol->ol_d_id = arg.d_id;
    tol->ol_w_id = arg.w_id;
    tol->ol_number = i;
    tol->ol_i_id = ol_i_id;
    tol->ol_supply_w_id = ol_supply_w_id;
    tol->ol_delivery_d = arg.o_entry_d;
    tol->ol_quantity = ol_quantity;
    tol->ol_amount = ol_amount;

    auto olk = orderlineKey(i, tol->ol_o_id, tol->ol_d_id, tol->ol_w_id);
    inscnt = g_wl->bi_orderline->insert(tx, olk ,rahol.row_id());
    if (inscnt != 1 || inscnt == BTreeIndex::kHaveToAbort) {
      // M_ASSERT (false, "bi_orderline: fail insert!\n");
      tx->aep_abort(); return Abort;
    }

  }
  return tx->aep_commit() == true? Commit: Abort;
}
RC tpcc_txn_man::run_order_status(tpcc_query* query) {
  auto& arg = query->args.order_status;
  tx->aep_begin(false); 

  int64_t row_id;
  uint64_t lookup_result;

  // 1.获取顾客信息
  if (!arg.by_last_name) {
    row_id = custKey(arg.c_id, arg.d_id, arg.w_id);
  }
  else {
    row_id = custNPKey(arg.d_id, arg.w_id, arg.c_last);
    auto i = g_wl->hi_customer_last->find(row_id);  // lastname输入总是正确的
    auto &v = i->second;
    row_id = v[v.size()/2];
  }
  lookup_result =
  g_wl->hi_customer->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
  if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
    M_ASSERT (false, "hi_customer: not found!\n");
    tx->aep_abort(); return Abort;
  }
  AepRowAccessHandle rahc(tx); 
  if (!rahc.aep_peek_row(g_wl->t_customer, 0, row_id, false, true, false) ||
    !rahc.aep_read_row()) {
    M_ASSERT (false, "hi_customer: rah not found!\n");
    tx->aep_abort(); return Abort;
  }
  tuple_customer *tc = (tuple_customer*)rahc.cdata();
  int64_t c_id = tc->c_id;

  // 2.获取客户最后的一次订单
  auto miok = orderCustKey(MAXIMUM_ORDERLINE, c_id, arg.d_id, arg.w_id);
  auto maok = orderCustKey(1, c_id, arg.d_id, arg.w_id);
  uint64_t op_result;
  int left;
  bool suspicious;
  auto make_value = [](uint64_t key) { return ~(key + 100); };
  auto scan_consumer_a = [&row_id](auto& k, auto& v) {
    (void)k;
    row_id = v;
    return false;
  };
  op_result = g_wl->bi_order_cust->lookup<BTreeRangeType::kInclusive,
                    BTreeRangeType::kOpen, false>(tx, miok, maok, false, scan_consumer_a);
  if (op_result == BTreeIndex::kHaveToAbort) {
    M_ASSERT (false, "bi_order_cust: HaveToAbort!\n");
    tx->aep_abort(); return Abort;
  }
  if (op_result == 1) {  // 找到了用户最近的1个订单

    // 3.找到客户最后一个订单，打印点订单信息
    AepRowAccessHandle raho(tx); 
    if (!raho.aep_peek_row(g_wl->t_order, 0, row_id, false, true, false) ||
      !raho.aep_read_row()) {
      // M_ASSERT (false, "hi_order: rah not found!\n");
      tx->aep_abort(); return Abort;
    }
    tuple_order *to = (tuple_order*)raho.cdata();
    auto o_id = to->o_id;
    
    // 获取orderline信息
    auto scan_consumer_b = [&row_id, &left, &suspicious, make_value](auto& k, auto& v) {
      (void)k;
      row_id = v;
      // 此处可根据row_id打印对应的orderline内容
      if (--left > 0)
        return true;
      else 
        return false;
    };
    miok = orderlineKey(1, o_id, arg.d_id, arg.w_id);
    maok = orderlineKey(15, o_id, arg.d_id, arg.w_id);
    op_result = g_wl->bi_orderline->lookup<BTreeRangeType::kInclusive,
                    BTreeRangeType::kInclusive, false>(tx, miok, maok, false, scan_consumer_b);
    if (op_result == BTreeIndex::kHaveToAbort) {
      M_ASSERT (false, "bi_orderline: HaveToAbort!\n");
      tx->aep_abort(); return Abort;
    }
  }

  return tx->aep_commit() == true? Commit: Abort;
}
RC tpcc_txn_man::run_delivery(tpcc_query* query) {
  auto& arg = query->args.delivery;
  tx->aep_begin(false); 

  int64_t row_id;
  uint64_t lookup_result, op_result;

  for (int64_t d_id =1; d_id<=DISTRICT_PER_WAREHOUSE; d_id++) {
    // 1.执行交付 = 删除最早的未交付订单
    auto mink = neworderKey(MAXIMUM_ORDERLINE, d_id, arg.w_id);
    auto mank = neworderKey(0, d_id, arg.w_id);
    auto scan_consumer_a = [&row_id](auto& k, auto& v) {
      (void)k;
      row_id = v;
      return false;
    };
    op_result = g_wl->bi_neworder->lookup<BTreeRangeType::kInclusive,
                    BTreeRangeType::kOpen, false>(tx, mink, mank, false, scan_consumer_a);
    if (op_result == BTreeIndex::kHaveToAbort) {
      // M_ASSERT (false, "bi_neworder: HaveToAbort!\n");
      tx->aep_abort(); return Abort;
    }
    if (op_result == 1) {  // 该街区有待投递订单，投递该订单
      // 2.读取neworder
      AepRowAccessHandle rahn(tx); 
      if (!rahn.aep_peek_row(g_wl->t_neworder, 0, row_id, false, true, true) ||
        !rahn.aep_read_row() || !rahn.aep_write_row(sizeof(tuple_neworder))) {
        // M_ASSERT (false, "bi_neworder: rah not found!, row_id=%ld\n", row_id);
        tx->aep_abort(); return Abort;
      }
      tuple_neworder *tn = (tuple_neworder*)rahn.data();

      // 3.更新订单表获知用户id
      auto row_id = orderKey(tn->no_o_id, tn->no_d_id, tn->no_w_id);
      lookup_result =
      g_wl->bi_order->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
      if (lookup_result != 1 || lookup_result == BTreeIndex::kHaveToAbort) {
        // M_ASSERT (false, "bi_order: not found!\n");
        tx->aep_abort(); return Abort;
      }
      AepRowAccessHandle raho(tx); 
      if (!raho.aep_peek_row(g_wl->t_order, 0, row_id, false, true, false) ||
         !raho.aep_read_row() || ! raho.aep_write_row(sizeof(tuple_order))) {
        // M_ASSERT (false, "bi_order: rah not found!\n");
        tx->aep_abort(); return Abort;
      }
      tuple_order *to = (tuple_order*)raho.data();
      to->o_carrier_id = arg.o_carrier_id;
      int64_t c_id = to->o_c_id;

      // 4.更新orderline数据表
      int left;
      int64_t ols[15], oli = 0; // 获取orderline id
      auto scan_consumer_b = [&row_id, &left, &ols, &oli] (auto& k, auto& v) {
        (void)k;
        row_id = v;
        ols[oli++] = row_id;
        if (--left > 0)
          return true;
        else 
          return false;
      };
      auto miok = orderlineKey(1,  tn->no_o_id, tn->no_d_id, tn->no_w_id);
      auto maok = orderlineKey(15, tn->no_o_id, tn->no_d_id, tn->no_w_id);
      op_result = g_wl->bi_orderline->lookup<BTreeRangeType::kInclusive,
                    BTreeRangeType::kInclusive, false>(tx, miok, maok, false, scan_consumer_b);
      if (op_result == BTreeIndex::kHaveToAbort) {
        // M_ASSERT (false, "bi_orderline: HaveToAbort!\n");
        tx->aep_abort(); return Abort;
      }
      double ol_total = 0.0;
      for (int64_t i=0; i<oli; i++) {
        row_id = ols[i];
        AepRowAccessHandle rahol(tx); 
        if (!rahol.aep_peek_row(g_wl->t_orderline, 0, row_id, false, true, true) ||
          !rahol.aep_read_row() || !rahol.aep_write_row(sizeof(tuple_orderline))) {
          // M_ASSERT (false, "hi_orderline: rah not found!\n");
          tx->aep_abort(); return Abort;
        }
        tuple_orderline *tol = (tuple_orderline*)rahol.data();
        tol->ol_delivery_d = arg.ol_delivery_d;
        ol_total += tol->ol_amount;
      }

      // 5.更新客户表数据
      row_id = custKey(c_id, d_id, arg.w_id);
      lookup_result =
      g_wl->hi_customer->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
      if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
        M_ASSERT (false, "hi_customer: not found!\n");
        tx->aep_abort(); return Abort;
      }
      AepRowAccessHandle rahc(tx); 
      if (!rahc.aep_peek_row(g_wl->t_customer, 0, row_id, false, true, true) ||
        !rahc.aep_read_row() || !rahc.aep_write_row(sizeof(tuple_customer))) {
        // M_ASSERT (false, "hi_customer: rah not found!\n");
        tx->aep_abort(); return Abort;
      }
      tuple_customer *tc = (tuple_customer*)rahc.cdata();
      tc->c_balance += ol_total;
      tc->c_delivery_cnt += 1;

      // 6.删除neworder
      if (!rahn.aep_delete_row()) {  // 此函数包括修改主索引的过程
        // M_ASSERT (false, "bi_neworder: rah not found!\n");
        tx->aep_abort(); return Abort;
      }
    }
  }

  return tx->aep_commit() == true? Commit: Abort;
}
RC tpcc_txn_man::run_stock_level(tpcc_query* query) {
  auto& arg = query->args.stock_level;
  tx->aep_begin(false); 
  int64_t row_id;
  uint64_t lookup_result, op_result;

  // 1.获取街区表 d_next_o_id;
  row_id = distKey(arg.d_id, arg.w_id);
  lookup_result =
  g_wl->hi_district->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
  if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
    // M_ASSERT (false, "hi_district: not found!\n");
    tx->aep_abort(); return Abort;
  }
  AepRowAccessHandle rahd(tx); 
  if (!rahd.aep_peek_row(g_wl->t_district, 0, row_id, false, true, false) ||
    !rahd.aep_read_row()) {
    // M_ASSERT (false, "hi_district: rah not found!\n");
    tx->aep_abort(); return Abort;
  }
  tuple_district *td = (tuple_district*)rahd.cdata();
  int64_t o_id = td->d_next_o_id;

  // 2.获取街区内20个订单库存不足的去重货物种类数量
  int left;
  int64_t ols[301], oli = 0; // 获取orderline id
  auto scan_consumer_b = [&row_id, &left, &ols, &oli] (auto& k, auto& v) {
    (void)k;
    row_id = v;
    ols[oli++] = row_id;
    if (--left > 0)
      return true;
    else 
      return false;
  };
  // 读取orderline中该街区最后20个orderline
  auto miok = orderlineKey(1,  o_id-1, arg.d_id, arg.w_id);
  auto maok = orderlineKey(15, o_id-20, arg.d_id, arg.w_id);
  op_result = g_wl->bi_orderline->lookup<BTreeRangeType::kInclusive,
                    BTreeRangeType::kInclusive, false>(tx, miok, maok, false, scan_consumer_b);
  if (op_result == BTreeIndex::kHaveToAbort) {
    M_ASSERT (false, "bi_orderline: HaveToAbort!\n");
    tx->aep_abort(); return Abort;
  }
  std::unordered_map<int64_t,int64_t> itemid2quantity; // 去重统计本仓库item的消耗量
  for (int64_t i=0; i<oli; i++) {
    row_id = ols[i];
    AepRowAccessHandle rahol(tx); 
    if (!rahol.aep_peek_row(g_wl->t_orderline, 0, row_id, false, true, false) ||
        !rahol.aep_read_row()) {
      // M_ASSERT (false, "hi_orderline: rah not found!\n");
      tx->aep_abort(); return Abort;
    }
    tuple_orderline *tol = (tuple_orderline*)rahol.cdata();
    if (tol->ol_supply_w_id == arg.w_id) {
      itemid2quantity[tol->ol_i_id] += tol->ol_quantity;
    }
  }
  // 和stock表做连接统计少于threshold的货物种类的数量
  int64_t scnt = 0;
  for (auto &it: itemid2quantity) {
    row_id = stockKey(it.first, arg.w_id);
    lookup_result =
    g_wl->hi_stock->lookup(tx, row_id, false,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
    if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
      // M_ASSERT (false, "hi_stock: not found!\n");
      tx->aep_abort(); return Abort;
    }
    AepRowAccessHandle rahs(tx); 
    if (!rahs.aep_peek_row(g_wl->t_stock, 0, row_id, false, true, false) ||
      !rahs.aep_read_row()) {
      // M_ASSERT (false, "hi_stock: rah not found!\n");
      tx->aep_abort(); return Abort;
    }
    tuple_stock *ts = (tuple_stock*)rahs.cdata();
    if (ts->s_quantity<arg.threshold) {
      scnt += 1;
    }
  }
  (void)scnt; // 保证上述统计过程不被优化
  return tx->aep_commit() == true? Commit: Abort;
}
#endif

