package com.zhangbao.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhangbao.gmall.realtime.app.func.DimSink;
import com.zhangbao.gmall.realtime.app.func.TableProcessFunction;
import com.zhangbao.gmall.realtime.bean.TableProcess;
import com.zhangbao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * 从kafka读取业务数据
 * @author: zhangbao
 * @date: 2021/8/15 21:10
 * @desc:
 **/
public class BaseDbTask {
    public static void main(String[] args) {
        //1.获取flink环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //设置并行度
        env.setParallelism(4);
        //设置检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9000/gmall/flink/checkpoint/baseDbApp"));
//        //指定哪个用户读取hdfs文件
//        System.setProperty("HADOOP_USER_NAME","zhangbao");
        //flink重启策略，
        // 如果开启上面的checkpoint，重启策略就是自动重启，程序有问题不会有报错，
        // 如果没有开启checkpoint，就不会自动重启，所以这里设置不需要重启，就可以查看错误信息
//        env.setRestartStrategy(RestartStrategies.noRestart());

        //2.从kafka获取topic数据
        String topic = "ods_base_db_m";
        String group = "base_db_app_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, group);
        DataStreamSource<String> jsonStrDs = env.addSource(kafkaSource);

        //3.对数据进行json转换
        SingleOutputStreamOperator<JSONObject> jsonObjDs = jsonStrDs.map(jsonObj -> JSON.parseObject(jsonObj));

        //4.ETL, table不为空，data不为空，data长度不能小于3
        SingleOutputStreamOperator<JSONObject> filterDs = jsonObjDs.filter(jsonObject -> jsonObject.getString("table") != null
                && jsonObject.getJSONObject("data") != null
                && jsonObject.getString("data").length() > 3);

        //5.动态分流，事实表写会kafka，维度表写入hbase
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};
        //创建自定义mapFunction函数，参数为分流标识
        SingleOutputStreamOperator<JSONObject> kafkaTag = filterDs.process(new TableProcessFunction(hbaseTag));
        //hbase分流数据
        DataStream<JSONObject> hbaseDs = kafkaTag.getSideOutput(hbaseTag);

        kafkaTag.print("主流数据（事实）DS --->>");
        hbaseDs.print("hbase（维度）分流数据DS");
        //6. 将维度表写入hbase中
        hbaseDs.addSink(new DimSink());

        //7. 将事实数据写回到kafka
        FlinkKafkaProducer<JSONObject> kafkaBySchema = MyKafkaUtil.getKafkaBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("kafka serialize open");
            }
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                String sinkTopic = jsonObject.getString("sink_table");
                return new ProducerRecord<>(sinkTopic, jsonObject.getJSONObject("data").toString().getBytes());
            }
        });
        kafkaTag.addSink(kafkaBySchema);

        try {
            env.execute("base db task");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
