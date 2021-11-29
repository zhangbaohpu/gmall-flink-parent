package com.zhangbao.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author zhangbaohpu
 * @date 2021/11/13 22:24
 * @desc 维度查询封装，底层调用PhoenixUtil
 */
public class DimUtil {


    public static JSONObject getDimInfo(String tableName, String key) {
        return getDimInfo(tableName,new Tuple2<>("id",key));
    }
    /**
     * 查询优化
     * redis缓存
     *      类型  string  list set zset hash
     * 这里使用key格式：
     *      key dim:table_name:value  示例：dim:base_trademark:13
     *      value   json字符串
     *      过期时间：24*3600
     */

    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>...
            colNameAndValue) {

        //组合查询条件
        String wheresql = new String(" where ");
        //redis key
        String redisKey = "dim:"+tableName+":";
        for (int i = 0; i < colNameAndValue.length; i++) {
            //获取查询列名以及对应的值
            Tuple2<String, String> nameValueTuple = colNameAndValue[i];
            String fieldName = nameValueTuple.f0;
            String fieldValue = nameValueTuple.f1;
            if (i > 0) {
                wheresql += " and ";
                redisKey += "_";
            }
            wheresql += fieldName + "='" + fieldValue + "'";
            redisKey += fieldValue;
        }
        Jedis jedis = null;
        String redisStr = null;
        JSONObject dimInfoJsonObj = null;
        try {
            jedis = RedisUtil.getJedis();
            redisStr = jedis.get(redisKey);
            dimInfoJsonObj = null;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询redis数据错误！");
        }
        //组合查询 SQL
        String sql = "select * from " + tableName + wheresql;
        System.out.println("查询维度 SQL:" + sql);

        if(redisStr!=null && redisStr.length()>0){
            dimInfoJsonObj = JSON.parseObject(redisStr);
        }else {
            //从phoenix中去数据

            List<JSONObject> dimList = PhoenixUtil.getList(sql, JSONObject.class);
            if (dimList != null && dimList.size() > 0) {
                //因为关联维度，肯定都是根据 key 关联得到一条记录
                dimInfoJsonObj = dimList.get(0);
                if(jedis!=null){
                    jedis.setex(redisKey,3600*24,dimInfoJsonObj.toString());
                }
            }else{
                System.out.println("维度数据未找到:" + sql);
            }
        }
        //关闭jedis
        if(jedis!=null){
            jedis.close();
        }

        return dimInfoJsonObj;
    }

    public static JSONObject getDimInfoNoCacheById(String tableName, String idValue) {
        return getDimInfoNoCache(tableName,new Tuple2<>("id",idValue));
    }
    //直接从 Phoenix 查询，没有缓存
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>...
            colNameAndValue) {
        //组合查询条件
        String wheresql = new String(" where ");
        for (int i = 0; i < colNameAndValue.length; i++) {
            //获取查询列名以及对应的值
            Tuple2<String, String> nameValueTuple = colNameAndValue[i];
            String fieldName = nameValueTuple.f0;
            String fieldValue = nameValueTuple.f1;
            if (i > 0) {
                wheresql += " and ";
            }
            wheresql += fieldName + "='" + fieldValue + "'";
        }
        //组合查询 SQL
        String sql = "select * from " + tableName + wheresql;
        System.out.println("查询维度 SQL:" + sql);
        JSONObject dimInfoJsonObj = null;
        List<JSONObject> dimList = PhoenixUtil.getList(sql, JSONObject.class);
        if (dimList != null && dimList.size() > 0) {
            //因为关联维度，肯定都是根据 key 关联得到一条记录
            dimInfoJsonObj = dimList.get(0);
        }else{
            System.out.println("维度数据未找到:" + sql);
        }
        return dimInfoJsonObj;
    }

    //根据 key 让 Redis 中的缓存失效
    public static void deleteCached( String tableName, String id){
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            // 通过 key 清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        JSONObject dimInfooNoCache = DimUtil.getDimInfoNoCache("base_trademark", Tuple2.of("id", "13"));
        JSONObject dimInfooNoCache = DimUtil.getDimInfo("base_trademark", Tuple2.of("id", "13"));
        System.out.println(dimInfooNoCache);
    }
}
