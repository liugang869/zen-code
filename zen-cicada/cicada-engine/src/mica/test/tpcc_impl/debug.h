#ifndef DEBUG
#define DEBUG

#include "wl.h"

void debug (void) {
  int64_t thdid = 0;
  ::mica::util::lcore.pin_thread(thdid);
  g_wl->db->activate(static_cast<uint16_t>(thdid));
  AepTransaction atx(g_wl->db->context(thdid)); 
  AepTransaction *tx = &atx; 
  Result cmt;

  tx->aep_begin(false);  // 是否是只读事务

  AepRowAccessHandle raha(tx), rahb(tx); 
  int64_t row_id;
  uint64_t lookup_result;

  row_id = 1;
  lookup_result =
  g_wl->hi_warehouse->lookup(tx, row_id, kSkipValidationForIndexAccess,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
  if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
    M_ASSERT (false, "hi_warehouse: not found!\n");
    tx->aep_abort(); return ;
  }
  if (!raha.aep_peek_row(g_wl->t_warehouse, 0, row_id, false, true, true) ||
    !raha.aep_read_row() || !raha.aep_write_row(sizeof(tuple_warehouse))) {
    M_ASSERT (false, "hi_warehouse: raha not found!\n");
    tx->aep_abort(); return ;
  }
  tuple_warehouse *tw = (tuple_warehouse*)raha.data();
  printf ("%s\n", tw->w_name);

  row_id = distKey(2, 1);
  lookup_result =
  g_wl->hi_district->lookup(tx, row_id, kSkipValidationForIndexAccess,
                             [&row_id](auto& k, auto& v) {
                             (void)k; row_id = v; return false;});
  if (lookup_result != 1 || lookup_result == HashIndex::kHaveToAbort) {
    M_ASSERT (false, "hi_district: not found!\n");
    tx->aep_abort(); return ;
  }
  printf("row_id: %ld\n", row_id);
  if (!rahb.aep_peek_row(g_wl->t_district, 0, row_id, false, true, true) ||
    !rahb.aep_read_row() || !rahb.aep_write_row(sizeof(tuple_district))) {
    M_ASSERT (false, "hi_district: rahb not found!\n");
    tx->aep_abort(); return ;
  }
  tuple_district *td = (tuple_district*)rahb.data();
  printf ("%s\n", td->d_name);

  g_wl->bi_neworder->insert(tx, 100, 10000);
  g_wl->bi_neworder->insert(tx, 101, 10001);
  g_wl->bi_neworder->insert(tx, 102, 10002);
  g_wl->bi_neworder->insert(tx, 200, 20000);
  g_wl->bi_neworder->insert(tx, 201, 20001);
  g_wl->bi_neworder->insert(tx, 202, 20002);

  tx->aep_commit(&cmt);  // 是否是只读事务

  tx->aep_begin(false);  // 是否是只读事务
  int left;
  bool suspicious;
  uint64_t mi = 203, ma = 300, op_result;
  auto make_value = [](uint64_t key) { return ~(key + 100); };
  auto scan_consumer = [&row_id, &left, &suspicious, make_value](auto& k, auto& v) {
    (void)k;
    row_id = v;
    printf("scan_consumer: %ld\n", row_id);
    if (row_id != make_value(k)) suspicious = true;
    // TODO: Perform range check.

    // return false;

    if (--left > 0)
      return true;
    else
      return false;
  };
  op_result = g_wl->bi_neworder->lookup<BTreeRangeType::kInclusive,
                                 BTreeRangeType::kOpen, false>(tx, mi, ma, false, scan_consumer);
  if (op_result != BTreeIndex::kHaveToAbort) {
    printf("op_result: %lu\n", op_result);
    printf("BTreeIndex: %ld suspicious: %d\n", row_id, suspicious);
  }

  tx->aep_commit(&cmt);  // 是否是只读事务

  g_wl->db->activate(static_cast<uint16_t>(0)); 
}

#endif
