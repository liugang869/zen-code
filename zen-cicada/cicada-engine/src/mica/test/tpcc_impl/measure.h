#ifndef MEASURE_H
#define MEASURE_H

#include "txn.h"

int64_t g_warm_que = MINIMUM_WARMING;
pthread_t t_ids[MAXIMUM_THREAD];
int64_t t_paras[MAXIMUM_THREAD];

pthread_barrier_t warm_bar, start_bar;

void* warm_running(void *thd_id) {
  int64_t thdid = *(int64_t*)thd_id;
  ::mica::util::lcore.pin_thread(thdid);
  g_wl->db->activate(static_cast<uint16_t>(thdid));
  AepTransaction tx(g_wl->db->context(thdid)); 
  tpcc_txn_man txn_man(thdid, &tx);
  tpcc_query *que = g_thds->get_queries(thdid);
  printf("thd_id: %ld que_num: \%ld\n", thdid, g_warm_que);
  pthread_barrier_wait(&warm_bar);
  for (int64_t i=0; i<g_warm_que; i++) {
    RC rc = txn_man.run_txn(que+i);

    // DEBUG
    // printf("thd: %ld txn: %ld\n", thdid, i);

    if (rc==Commit) {
      committed_query[thdid][(que+i)->type] += 1;
    }
  }

  g_wl->db->deactivate(static_cast<uint16_t>(thdid));
  return nullptr;
}
void* meas_running(void *thd_id) {
  int64_t thdid = *(int64_t*)thd_id;
  ::mica::util::lcore.pin_thread(thdid);
  g_wl->db->activate(static_cast<uint16_t>(thdid));
  AepTransaction tx(g_wl->db->context(thdid)); 
  tpcc_txn_man txn_man(thdid, &tx);
  tpcc_query *que = g_thds->get_queries(thdid);
  printf("thd_id: %ld que_num: \%ld\n", thdid, g_num_que-g_warm_que);
  pthread_barrier_wait(&start_bar);
  for (int64_t i=g_warm_que; i<g_num_que; i++) {
    RC rc = txn_man.run_txn(que+i);

    // DEBUG
    // printf("thd: %ld txn: %ld\n", thdid, i);

    if (rc==Commit) {
      committed_query[thdid][(que+i)->type] += 1;
    }
  }

  g_wl->db->deactivate(static_cast<uint16_t>(thdid));
  return nullptr;
}
void measure (void) {
  for(int64_t i=0; i<g_num_thd; i++) {
    t_paras[i] = i; g_history[i] = 0;
  }

  printf ("warming ...\n");
  pthread_barrier_init(&warm_bar, nullptr, g_num_thd+1);
  for (int64_t i=0; i<g_num_thd; i++)
    pthread_create(&t_ids[i], nullptr, warm_running, &t_paras[i]);
  pthread_barrier_wait(&warm_bar);
  for (int64_t i=0; i<g_num_thd; i++)
    pthread_join(t_ids[i], nullptr);
  pthread_barrier_destroy(&warm_bar);
 
  printf ("measuring ...\n");
  for (int64_t i=0; i<g_num_thd; i++)
    for (int64_t j=0; j<TPCC_TXN_TYPES; j++)
      committed_query[i][j] = 0;
  struct timeval ts, te; 
  pthread_barrier_init(&start_bar, nullptr, g_num_thd+1);
  for (int64_t i=0; i<g_num_thd; i++)
    pthread_create(&t_ids[i], nullptr, meas_running, &t_paras[i]);
  pthread_barrier_wait(&start_bar);
  gettimeofday(&ts, nullptr);
  for (int64_t i=0; i<g_num_thd; i++)
    pthread_join(t_ids[i], nullptr);
  gettimeofday(&te, nullptr);
  pthread_barrier_destroy(&start_bar);

  int64_t num_txn = (g_num_que-g_warm_que)*g_num_thd;
  int64_t elapse = (te.tv_sec-ts.tv_sec)*1000000+(te.tv_usec-ts.tv_usec);  // 微秒
  double mtps = ((double)num_txn)/elapse;
  int64_t ctxn = 0;
  for (int64_t i=0; i<g_num_thd; i++)
    for (int64_t j=0; j<TPCC_TXN_TYPES; j++)
      ctxn += committed_query[i][j];
  double crate = ((double)ctxn)/num_txn*100;
  printf("throughput:  %.3lf MTPS\n", mtps);
  printf("commit rate: %.2lf \%\n", crate);
}

#endif
