package com.zhangbao.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhangbao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: zhangbao
 * @date: 2021/9/12 19:51
 * @desc: uv 计算
 **/

public class UniqueVisitApp {
    public static void main(String[] args) {
        //webui模式，需要添加pom依赖
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createLocalEnvironment();
        //设置并行度
        env.setParallelism(4);
        //设置检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9000/gmall/flink/checkpoint/uniqueVisit"));
        //指定哪个用户读取hdfs文件
        System.setProperty("HADOOP_USER_NAME","zhangbao");

        //从kafka读取数据源
        String sourceTopic = "dwd_page_log";
        String group = "unique_visit_app_group";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, group);
        DataStreamSource<String> kafkaDs = env.addSource(kafkaSource);

        //数据转换
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDs.map(obj -> JSON.parseObject(obj));

        //按照设备id分组
        KeyedStream<JSONObject, String> keyByMid = jsonObjDs.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //过滤
        SingleOutputStreamOperator<JSONObject> filterDs = keyByMid.filter(new RichFilterFunction<JSONObject>() {
            ValueState<String> lastVisitDate = null;
            SimpleDateFormat sdf = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化时间
                sdf = new SimpleDateFormat("yyyyMMdd");
                //初始化状态
                ValueStateDescriptor<String> lastVisitDateDesc = new ValueStateDescriptor<>("lastVisitDate", String.class);
                //统计日活dau，状态数据保存一天，过一天即失效
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                lastVisitDateDesc.enableTimeToLive(stateTtlConfig);
                this.lastVisitDate = getRuntimeContext().getState(lastVisitDateDesc);

            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                //上一个页面如果有值，则不是首次访问
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if(lastPageId != null && lastPageId.length()>0){
                    return false;
                }
                //获取用户访问日期
                Long ts = jsonObject.getLong("ts");
                String mid = jsonObject.getJSONObject("common").getString("mid");
                String lastDate = sdf.format(new Date(ts));
                //获取状态日期
                String lastDateState = lastVisitDate.value();
                if(lastDateState != null && lastDateState.length()>0 && lastDateState.equals(lastDate)){
                    System.out.println(String.format("已访问! mid：%s，lastDate：%s",mid,lastDate));
                    return false;
                }else {
                    lastVisitDate.update(lastDate);
                    System.out.println(String.format("未访问! mid：%s，lastDate：%s",mid,lastDate));
                    return true;
                }
            }
        });

        filterDs.print("filterDs >>>");

        //向kafka写回，将数据转换成json
        SingleOutputStreamOperator<String> jsonDs = filterDs.map(jsonObj -> jsonObj.toJSONString());

        //写回到kafka的dwm层
        jsonDs.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        try {
            env.execute("task uniqueVisitApp");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
