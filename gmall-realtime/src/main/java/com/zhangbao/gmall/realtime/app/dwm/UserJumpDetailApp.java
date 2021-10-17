package com.zhangbao.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhangbao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author zhangbao
 * @date 2021/10/17 10:38
 * @desc
 */
public class UserJumpDetailApp {
    public static void main(String[] args) {
        //webui模式，需要添加pom依赖
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createLocalEnvironment();
        //设置并行度
        env.setParallelism(4);
        //设置检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9000/gmall/flink/checkpoint/userJumpDetail"));
//        //指定哪个用户读取hdfs文件
//        System.setProperty("HADOOP_USER_NAME","zhangbao");

        //从kafka读取数据源
        String sourceTopic = "dwd_page_log";
        String group = "user_jump_detail_app_group";
        String sinkTopic = "dwm_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, group);
        DataStreamSource<String> jsonStrDs = env.addSource(kafkaSource);

        /*//测试数据
        DataStream<String> jsonStrDs = env
         .fromElements(
                "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",

                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                        "\"home\"},\"ts\":15000} ",

                "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                        "\"detail\"},\"ts\":30000} "
        );
        dataStream.print("in json:");*/

        //对读取到的数据进行结构转换
        SingleOutputStreamOperator<JSONObject> jsonObjDs = jsonStrDs.map(jsonStr -> JSON.parseObject(jsonStr));

//        jsonStrDs.print("user jump detail >>>");
        //从flink1.12开始，时间语义默认是事件时间，不需要额外指定，如果是之前的版本，则要按以下方式指定事件时间语义
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //指定事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDs = jsonObjDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }
        ));

        //按照mid分组
        KeyedStream<JSONObject, String> ketByDs = jsonObjWithTSDs.keyBy(
                jsonObject -> jsonObject.getJSONObject("common").getString("mid")
        );

        /**
         * flink CEP表达式
         * 跳出规则，满足两个条件：
         *  1.第一次访问的页面：last_page_id == null
         *  2.第一次访问的页面在10秒内，没有进行其他操作，没有访问其他页面
         */
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where( // 1.第一次访问的页面：last_page_id == null
                    new SimpleCondition<JSONObject>() {
                        @Override
                        public boolean filter(JSONObject jsonObject) throws Exception {
                            String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                            System.out.println("first page >>> "+lastPageId);
                            if (lastPageId == null || lastPageId.length() == 0) {
                                return true;
                            }
                            return false;
                        }
                    }
                ).next("next")
                .where( //2.第一次访问的页面在10秒内，没有进行其他操作，没有访问其他页面
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {
                                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                System.out.println("next page >>> "+pageId);
                                if(pageId != null && pageId.length()>0){
                                    return true;
                                }
                                return false;
                            }
                        }
                //时间限制模式，10S
                ).within(Time.milliseconds(10000));

        //将cep表达式运用到流中，筛选数据
        PatternStream<JSONObject> patternStream = CEP.pattern(ketByDs, pattern);

        //从筛选的数据中再提取数据超时数据，放到侧输出流中
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut"){};
        SingleOutputStreamOperator<Object> outputStreamDS = patternStream.flatSelect(
                timeOutTag,
                //获取超时数据
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                        List<JSONObject> first = map.get("first");
                        for (JSONObject jsonObject : first) {
                            System.out.println("time out date >>> "+jsonObject.toJSONString());
                            //所有 out.collect 的数据都被打上了超时标记
                            collector.collect(jsonObject.toJSONString());
                        }
                    }
                },
                //获取未超时数据
                new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<Object> collector) throws Exception {
                        //不超时的数据不提取，所以这里不做操作
                    }
                }
        );

        //获取侧输出流的超时数据
        DataStream<String> timeOutDs = outputStreamDS.getSideOutput(timeOutTag);
        timeOutDs.print("jump >>> ");

        //将跳出数据写回到kafka
        timeOutDs.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        try {
            env.execute("user jump detail task");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
