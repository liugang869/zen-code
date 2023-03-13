#ifndef TUPLE_H
#define TUPLE_H

#include <stdlib.h>

struct tuple_warehouse {
  int64_t w_id;
  char w_name[10];
  char w_street_1[20];
  char w_street_2[20];
  char w_city[20];
  char w_state[2];
  char w_zip[9];
  double w_tax;
  double w_ytd;
};

struct tuple_district {
  int64_t d_id;
  int64_t d_w_id;
  char d_name[10];
  char d_street_1[20];
  char d_street_2[20];
  char d_city[20];
  char d_state[2];
  char d_zip[9];
  double d_tax;
  double d_ytd;
  int64_t d_next_o_id;
};

struct tuple_customer {
  int64_t c_id;
  int64_t c_d_id;
  int64_t c_w_id;
  char c_first[16];
  char c_middle[2];
  char c_last[16];
  char c_street_1[20];
  char c_street_2[20];
  char c_city[20];
  char c_state[2];
  char c_zip[9];
  char c_phone[16];
  int64_t c_since;
  char c_credit[2];
  double c_credit_lim;
  double c_discount;
  double c_balance;
  double c_ytd_payment;
  int64_t c_payment_cnt;
  int64_t c_delivery_cnt;
  char c_data[500];
};

struct tuple_history {
  int64_t h_c_id;
  int64_t h_c_d_id;
  int64_t h_c_w_id;
  int64_t h_d_id;
  int64_t h_w_id;
  int64_t h_date;
  double h_amount;
  char h_data[24];
};

struct tuple_neworder {
  int64_t no_o_id;
  int64_t no_d_id;
  int64_t no_w_id;
};

struct tuple_order {
  int64_t o_id;
  int64_t o_c_id;
  int64_t o_d_id;
  int64_t o_w_id;
  int64_t o_entry_d;
  int64_t o_carrier_id;
  int64_t o_ol_cnt;
  int64_t o_all_local;
};

struct tuple_orderline {
  int64_t ol_o_id;
  int64_t ol_d_id;
  int64_t ol_w_id;
  int64_t ol_number;
  int64_t ol_i_id;
  int64_t ol_supply_w_id;
  int64_t ol_delivery_d;
  int64_t ol_quantity;
  double ol_amount;
  char ol_dist_info[24];
};

struct tuple_item {
  int64_t i_id;
  int64_t i_im_id;
  char i_name[24];
  double i_price;
  char i_data[50];
};

struct tuple_stock {
  int64_t s_i_id;
  int64_t s_w_id;
  int64_t s_quantity;
  char s_dist_01[24];
  char s_dist_02[24];
  char s_dist_03[24];
  char s_dist_04[24];
  char s_dist_05[24];
  char s_dist_06[24];
  char s_dist_07[24];
  char s_dist_08[24];
  char s_dist_09[24];
  char s_dist_10[24];
  double s_ytd;
  int64_t s_order_cnt;
  int64_t s_remote_cnt;
  char s_data[50];
};

#endif
