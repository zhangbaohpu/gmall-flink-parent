package com.zhangbao.gmall.realtime.common;

/**
 * @author: zhangbao
 * @date: 2021/8/27 23:40
 * @desc:
 **/
public class GmallConfig {

    //hbase数据库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    //phoenix连接地址
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181";

    //phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

}
