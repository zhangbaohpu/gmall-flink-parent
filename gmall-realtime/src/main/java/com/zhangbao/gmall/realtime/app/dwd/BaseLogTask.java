package com.zhangbao.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhangbao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author: zhangbao
 * @date: 2021/6/18 23:29
 * @desc:
 **/
public class BaseLogTask {
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    private static final String TOPIC_PAGE = "dwd_page_log";
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度，即kafka分区数
        env.setParallelism(4);
        //添加checkpoint，每5秒执行一次
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9000/gmall/flink/checkpoint/baseLogAll"));
//        //指定哪个用户读取hdfs文件
//        System.setProperty("HADOOP_USER_NAME","zhangbao");

        //添加数据源，来至kafka的数据
        String topic = "ods_base_log";
        String group = "base_log_app_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, group);
        DataStreamSource<String> kafkaDs = env.addSource(kafkaSource);
        //对格式进行转换
        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaDs.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONObject.parseObject(s);
            }
        });
        jsonDs.print("json >>> --- ");
        /**
         * 识别新老访客，前端会对新老客状态进行记录，可能不准，这里再次确认
         * 保存mid某天状态情况（将首次访问日期作为状态保存），等后面设备在有日志过来，从状态中获取日期和日志产生日期比较，
         * 如果状态不为空，并且状态日期和当前日期不相等，说明是老访客，如果is_new标记是1，则对其状态进行修复
         */
        //根据id对日志进行分组
        KeyedStream<JSONObject, String> midKeyedDs = jsonDs.keyBy(data -> data.getJSONObject("common").getString("mid"));
        //新老访客状态修复，状态分为算子状态和键控状态，我们这里记录某一个设备状态，使用键控状态比较合适
        SingleOutputStreamOperator<JSONObject> midWithNewFlagDs = midKeyedDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            //定义mid状态
            private ValueState<String> firstVisitDateState;
            //定义日期格式化
            private SimpleDateFormat sdf;
            //初始化方法
            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("newMidDateState", String.class));
                sdf = new SimpleDateFormat("yyyyMMdd");
            }
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //获取当前mid状态
                String is_new = jsonObject.getJSONObject("common").getString("is_new");
                //获取当前日志时间戳
                Long ts = jsonObject.getLong("ts");
                if ("1".equals(is_new)) {
                    //访客日期状态
                    String stateDate = firstVisitDateState.value();
                    String nowDate = sdf.format(new Date());
                    if (stateDate != null && stateDate.length() != 0 && !stateDate.equals(nowDate)) {
                        //是老客
                        is_new = "0";
                        jsonObject.getJSONObject("common").put("is_new", is_new);
                    } else {
                        //新访客
                        firstVisitDateState.update(nowDate);
                    }
                }
                return jsonObject;
            }
        });

//        midWithNewFlagDs.print();

        /**
         * 根据日志数据内容,将日志数据分为 3 类, 页面日志、启动日志和曝光日志。页面日志
         * 输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流
         * 侧输出流：1接收迟到数据，2分流
         */
        //定义启动侧输出流标签，加大括号为了生成相应类型
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        //定义曝光侧输出流标签
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        SingleOutputStreamOperator<String> pageDs = midWithNewFlagDs.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                        String dataStr = jsonObject.toString();
                        JSONObject startJson = jsonObject.getJSONObject("start");
                        //判断是否启动日志
                        if (startJson != null && startJson.size() > 0) {
                            context.output(startTag, dataStr);
                        } else {
                            //如果不是曝光日志，则是页面日志，输出到主流
                            collector.collect(dataStr);

                            //判断是否曝光日志(曝光日志也是页面日志的一种)
                            JSONArray jsonArray = jsonObject.getJSONArray("displays");
                            if (jsonArray != null && jsonArray.size() > 0) {
                                //给每一条曝光事件加pageId
                                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                //遍历输出曝光日志
                                for (int i = 0; i < jsonArray.size(); i++) {
                                    JSONObject disPlayObj = jsonArray.getJSONObject(i);
                                    disPlayObj.put("page_id", pageId);
                                    context.output(displayTag, disPlayObj.toString());
                                }
                            }
//                            else {
//                                //如果不是曝光日志，则是页面日志，输出到主流
//                                collector.collect(dataStr);
//                            }
                        }
                    }
                }
        );

        //获取侧输出流
        DataStream<String> startDs = pageDs.getSideOutput(startTag);
        DataStream<String> disPlayDs = pageDs.getSideOutput(displayTag);
        //打印输出
        startDs.print("start>>>");
        disPlayDs.print("display>>>");
        pageDs.print("page>>>");

        /**
         * 将不同流的日志数据发送到指定的kafka主题
         */
        startDs.addSink(MyKafkaUtil.getKafkaSink(TOPIC_START));
        disPlayDs.addSink(MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY));
        pageDs.addSink(MyKafkaUtil.getKafkaSink(TOPIC_PAGE));

        try {
            //执行
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
























