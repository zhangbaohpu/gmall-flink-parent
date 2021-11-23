package com.zhangbao.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author zhangbao
 * @date 2021/10/25 19:55
 * @desc 订单
 */
@Data
public class OrderInfo {
    Long id;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time;
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;
    Long create_ts;
}
