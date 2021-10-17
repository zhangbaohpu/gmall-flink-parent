package com.zhangbao.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.zhangbao.gmall.realtime.common.GmallConfig;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * @author: zhangbao
 * @date: 2021/9/4 12:23
 * @desc: 将维度表写入hbase中
 **/
@Log4j2
public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("建立 phoenix 连接...");
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        log.info("phoenix 连接成功！");
    }

    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        String sinkTable = jsonObject.getString("sink_table");
        JSONObject data = jsonObject.getJSONObject("data");
        PreparedStatement ps = null;
        if(data!=null && data.size()>0){
            try {
                //生成phoenix的upsert语句，这个包含insert和update操作
                String sql = generateUpsert(data,sinkTable.toUpperCase());
                log.info("开始执行 phoenix sql -->{}",sql);
                ps = conn.prepareStatement(sql);
                ps.executeUpdate();
                conn.commit();
                log.info("执行 phoenix sql 成功");
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                throw new RuntimeException("执行 phoenix sql 失败！");
            }finally {
                if(ps!=null){
                    ps.close();
                }
            }
        }
    }

    //生成 upsert sql
    private String generateUpsert(JSONObject data, String sinkTable) {
        StringBuilder sql = new StringBuilder();
        //upsert into scheme.table(id,name) values('11','22')
        sql.append("upsert into "+GmallConfig.HBASE_SCHEMA+"."+sinkTable+"(");
        //拼接列名
        sql.append(StringUtils.join(data.keySet(),",")).append(")");
        //填充值
        sql.append("values('"+ StringUtils.join(data.values(),"','")+"')");
        return sql.toString();
    }
}
