package com.zhangbao.gmall.realtime.utils;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSONObject;
import com.zhangbao.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.io.PrintStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangbaohpu
 * @date 2021/11/13 21:26
 * @desc phoenix 工具类，操作hbase数据
 */
public class PhoenixUtil {

    private static Connection conn = null;

    public static void init(){
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("连接phoenix失败 -> " + e.getMessage());
        }
    }

    public static <T> List<T> getList(String sql, Class<T> clazz){
        if(conn == null){
            init();
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> resultList = new ArrayList<>();
        try {
            //获取数据库对象
            ps = conn.prepareStatement(sql);
            //执行sql语句
            rs = ps.executeQuery();
            //获取元数据
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()){
                //创建对象
                T rowObj = clazz.newInstance();
                //动态给对象赋值
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    BeanUtils.setProperty(rowObj,metaData.getColumnName(i),rs.getObject(i));
                }
                resultList.add(rowObj);
            }
        }catch (Exception e){
            throw new RuntimeException("phoenix 查询失败 -> " + e.getMessage());
        }
        /*finally {
            if(rs!=null){
                try {
                    rs.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if(ps!=null){
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if(conn!=null){
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }*/
        return resultList;
    }

    public static void main(String[] args) {
        String sql = "select * from DIM_USER_INFO where id='28786'";
        System.out.println(getList(sql,JSONObject.class));
    }

}
