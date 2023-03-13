#ifndef HELPER_H
#define HELPER_H

#define TPCC_TXN_TYPES (5)
#define DISTRICT_PER_WAREHOUSE (10L)
#define CUSTOMER_PER_DISTRICT (3000L)
#define ITEM_SIZE (100000L)
#define MAXIMUM_ITEM_PER_NEWORDER (15)
#define MAXIMUM_ORDERLINE (1UL<<32)
#define MAXIMUM_ORDER (1UL<<32)
#define MAXIMUM_NEWORDER (1UL<<32)
#define MAXIMUM_HISTORY (1UL<<32)
#define MAXIMUM_THREAD (64L)
#define MINIMUM_WARMING (10000)

#include <city.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#define M_ASSERT(cond, ...) \
	if (!(cond)) {\
		printf("ASSERTION FAILURE [%s : %d] ", \
		__FILE__, __LINE__); \
		printf(__VA_ARGS__);\
		assert(false);\
	}

int64_t g_num_wh, g_num_thd, g_num_que;

typedef u_int64_t uint64_t;
uint64_t C_255, C_1023, C_8191;
drand48_data** tpcc_buffer;

uint64_t RAND(uint64_t max, uint64_t thd_id) {
  int64_t rint64 = 0;
  lrand48_r(tpcc_buffer[thd_id], &rint64);
  return rint64 % max;
}
uint64_t URand(uint64_t x, uint64_t y, uint64_t thd_id) {
  return x + RAND(y - x + 1, thd_id);
}
void InitNURand(uint64_t thd_id) {
  C_255 = (uint64_t)URand(0, 255, thd_id);
  C_1023 = (uint64_t)URand(0, 1023, thd_id);
  C_8191 = (uint64_t)URand(0, 8191, thd_id);
}
uint64_t NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id) {
  int C = 0;
  switch (A) {
    case 255:
      C = C_255;
      break;
    case 1023:
      C = C_1023;
      break;
    case 8191:
      C = C_8191;
      break;
    default:
      M_ASSERT(false, "Error! NURand\n");
      exit(-1);
  }
  return (((URand(0, A, thd_id) | URand(x, y, thd_id)) + C) % (y - x + 1)) + x;
}

uint64_t itemKey(uint64_t i_id) { return i_id; }
uint64_t warehouseKey(uint64_t w_id) { return w_id; }
uint64_t distKey(uint64_t d_id, uint64_t d_w_id) {
  return d_w_id * DISTRICT_PER_WAREHOUSE + d_id;
}
uint64_t custKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id) {
  return distKey(c_d_id, c_w_id) * CUSTOMER_PER_DISTRICT + c_id;
}
uint64_t custNPKey(uint64_t c_d_id, uint64_t c_w_id, const char* c_last) {
  return CityHash64(c_last, strlen(c_last)) * g_num_wh * DISTRICT_PER_WAREHOUSE +
         distKey(c_d_id, c_w_id);
}
uint64_t stockKey(uint64_t s_i_id, uint64_t s_w_id) {
  return s_w_id * ITEM_SIZE + s_i_id;
}
uint64_t orderKey(int64_t o_id, uint64_t o_d_id, uint64_t o_w_id) {
  // Use negative o_id to allow reusing the current index interface.
  return distKey(o_d_id, o_w_id) * MAXIMUM_ORDERLINE + (MAXIMUM_ORDERLINE - o_id);
}
uint64_t orderCustKey(int64_t o_id, uint64_t o_c_id, uint64_t o_d_id,
                      uint64_t o_w_id) {
  // Use negative o_id to allow reusing the current index interface.
  return distKey(o_d_id, o_w_id) * CUSTOMER_PER_DISTRICT * MAXIMUM_ORDERLINE +
         o_c_id * MAXIMUM_ORDERLINE + (MAXIMUM_ORDERLINE - o_id);
}
uint64_t neworderKey(int64_t o_id, uint64_t o_d_id, uint64_t o_w_id) {
  return distKey(o_d_id, o_w_id) * MAXIMUM_ORDERLINE + (MAXIMUM_ORDERLINE - o_id);
}
uint64_t orderlineKey(uint64_t ol_number, int64_t ol_o_id, uint64_t ol_d_id,
                      uint64_t ol_w_id) {
  // Use negative ol_o_id to allow reusing the current index interface.
  return distKey(ol_d_id, ol_w_id) * MAXIMUM_ORDERLINE * 15 +
         (MAXIMUM_ORDERLINE - ol_o_id) * 15 + ol_number;
}
uint64_t Lastname(uint64_t num, char* name) {
  static const char* n[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                            "ESE", "ANTI",  "CALLY", "ATION", "EING"};
  strcpy(name, n[num / 100]);
  strcat(name, n[(num / 10) % 10]);
  strcat(name, n[num % 10]);
  return strlen(name);
}
uint64_t MakeAlphaString(int min, int max, char* str, uint64_t thd_id) {
  char char_list[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
                      'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                      'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
                      'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
                      'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
                      'U', 'V', 'W', 'X', 'Y', 'Z'};
  uint64_t cnt = URand(min, max, thd_id);
  for (uint32_t i = 0; i < cnt; i++) str[i] = char_list[URand(0L, 60L, thd_id)];
  for (int i = cnt; i < max; i++) str[i] = '\0';

  return cnt;
}
uint64_t MakeNumberString(int min, int max, char* str, uint64_t thd_id) {
  uint64_t cnt = URand(min, max, thd_id);
  for (uint32_t i = 0; i < cnt; i++) {
    uint64_t r = URand(0L, 9L, thd_id);
    str[i] = '0' + r;
  }
  return cnt;
}
#endif
