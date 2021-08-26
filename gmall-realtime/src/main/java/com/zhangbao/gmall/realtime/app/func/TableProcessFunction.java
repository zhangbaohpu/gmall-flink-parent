package com.zhangbao.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.zhangbao.gmall.realtime.bean.TableProcess;
import com.zhangbao.gmall.realtime.utils.MysqlUtil;
import lombok.extern.log4j.Log4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: zhangbao
 * @date: 2021/8/26 23:24
 * @desc:
 **/
@Log4j
public class TableProcessFunction extends ProcessFunction<JSONObject,String> {
    //定义输出流标记
    private OutputTag<JSONObject> outputTag;
    //定义配置信息
    private Map<String , TableProcess> tableProcessMap = new HashMap<>();
    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //只执行一次
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化配置信息
        log.info("查询配置表信息");
        refreshDate();
    }

    private void refreshDate() {
        List<TableProcess> processList = MysqlUtil.getList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : processList) {
            String sourceTable = tableProcess.getSourceTable();
            String operateType = tableProcess.getOperateType();
            String key = sourceTable+":"+operateType;
            tableProcessMap.put(key,tableProcess);
        }
    }

    //每进来一个元素，执行一次
    @Override
    public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

    }
}
