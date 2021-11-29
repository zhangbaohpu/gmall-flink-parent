package com.zhangbao.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author zhangbaohpu
 * @date 2021/11/28 12:34
 * @desc  维度关联接口
 */
public interface DimJoinFunction<T> {

    //根据流中获取主键
    String getKey(T obj);

    //维度关联
    void join(T stream, JSONObject dimInfo);
}
