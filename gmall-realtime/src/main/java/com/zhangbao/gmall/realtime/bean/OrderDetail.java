package com.zhangbao.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author zhangbao
 * @date 2021/10/25 19:55
 * @desc 订单明细
 */
@Data
public class OrderDetail {
    Long id;
    Long order_id ;
    Long sku_id;
    BigDecimal order_price ;
    Long sku_num ;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;
}
