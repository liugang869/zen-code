
#include <iostream>
#include "tpcc_impl/measure.h"
#include "tpcc_impl/debug.h"

using namespace std;

int main(int argc, char *argv[]) {
  // 默认值
  int64_t num_wh = 64;   // 仓库数量，设置更大的warehosue时如果崩溃请调整AEP_ALLOC_SIZE
  int64_t num_thd = 16;  // 线程数量，需要小于等于MAXIMUM_THREAD
  int64_t num_que = 20000;  // 每个线程运行的事务数量，需要大于num_warm
  int64_t num_warm = 10000; // 其中用于预执行的事务数量，需要大于等于MINIMUM_WARMING

  if (argc == 5) {
    num_wh = atol(argv[1]);
    num_thd = atol(argv[2]);
    num_que = atol(argv[3]);
    num_warm = atol(argv[4]);
  }
  else if (argc != 1) {
    cout<<"Command example: "<<endl;
    cout<<"./test_tpcc num_of_warehouse num_of_thread number_of_transaction_per_thread number_of_transaction_per_thread_for_warmup"<<endl;
    return 0;
  }

  // 按照参数运行程序

  assert(num_wh>=1);
  assert(num_thd <= MAXIMUM_THREAD);
  assert(num_que >= num_warm);
  assert(num_warm >= MINIMUM_WARMING);

  cout<<"tpcc running ..."<<endl;

  g_num_wh = num_wh;
  g_num_thd = num_thd;
  g_num_que = num_que;
  g_warm_que = num_warm;

  g_wl = new tpcc_wl;      // 仓库数量，线程数量 
  g_thds = new tpcc_thds;
  assert(g_wl != nullptr);
  assert(g_thds != nullptr);

  // 调试
  // debug();

  // 测量
  measure();

  return 0;
}
