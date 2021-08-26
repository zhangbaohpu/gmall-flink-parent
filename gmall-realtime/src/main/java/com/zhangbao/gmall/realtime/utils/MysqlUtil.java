package com.zhangbao.gmall.realtime.utils;

import com.google.common.base.CaseFormat;
import com.zhangbao.gmall.realtime.bean.TableProcess;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.reflect.FieldUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: zhangbao
 * @date: 2021/8/22 13:09
 * @desc:
 **/
public class MysqlUtil {

    private static final String DRIVER_NAME = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://192.168.88.71:3306/gmall2021_realtime?characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8";
    private static final String USER_NAME = "root";
    private static final String USER_PWD = "123456";

    public static void main(String[] args) {
        String sql = "select * from table_process";
        List<TableProcess> list = getList(sql, TableProcess.class, true);
        for (TableProcess tableProcess : list) {
            System.out.println(tableProcess.toString());
        }
    }

    public static <T> List<T> getList(String sql,Class<T> clz, boolean under){
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Class.forName(DRIVER_NAME);
            conn = DriverManager.getConnection(URL, USER_NAME, USER_PWD);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            List<T> resultList = new ArrayList<>();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()){
                System.out.println(rs.getObject(1));
                T obj = clz.newInstance();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    String propertyName = "";
                    if(under){
                        //指定数据库字段转换为驼峰命名法，guava工具类
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                   //通过guava工具类设置属性值
                    BeanUtils.setProperty(obj,propertyName,rs.getObject(i));
                }
                resultList.add(obj);
            }
            return resultList;
        } catch (Exception throwables) {
            throwables.printStackTrace();
            new RuntimeException("msql 查询失败！");
        } finally {
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
        }
        return null;
    }
}
