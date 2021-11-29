package com.zhangbao.gmall.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.zhangbao.gmall.realtime.utils.DimUtil;
import com.zhangbao.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

/**
 * @author zhangbaohpu
 * @date 2021/11/28 12:24
 * @desc 通用的维度关联查询接口
 * 模板方法设计模式
 *   在父类中只定义方法的声明
 *   具体实现由子类完成
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {

    private String tableName;
    
    private static ExecutorService executorPool;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池
        executorPool = ThreadPoolUtil.getPoolExecutor();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorPool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();
                    String key = getKey(obj);
                    //获取维度信息
                    JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);

                    //关联维度
                    if (dimInfoJsonObj != null){
                        join(obj,dimInfoJsonObj);
                    }
                    long end = System.currentTimeMillis();
                    System.out.println("关联维度数据，耗时："+(end - start)+" 毫秒。");
                    resultFuture.complete(Arrays.asList(obj));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(tableName+"维度查询失败");
                }
            }
        });
    }
}
