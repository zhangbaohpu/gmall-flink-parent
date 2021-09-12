package com.zhangbao.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhangbao.gmall.realtime.app.func.TableProcessFunction;
import com.zhangbao.gmall.realtime.bean.TableProcess;
import com.zhangbao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

/**
 * 从kafka读取业务数据
 * @author: zhangbao
 * @date: 2021/8/15 21:10
 * @desc:
 **/
public class Test {
    public static void main(String[] args) {
        //1.获取flink环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        //设置并行度
        env.setParallelism(4);
        //设置检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9000/gmall/flink/checkpoint/baseDbApp"));
        //指定哪个用户读取hdfs文件
        System.setProperty("HADOOP_USER_NAME","zhangbao");
        //flink重启策略，
        // 如果开启上面的checkpoint，重启策略就是自动重启，程序有问题不会有报错，
        // 如果没有开启checkpoint，就不会自动重启，所以这里设置不需要重启，就可以查看错误信息
        env.setRestartStrategy(RestartStrategies.noRestart());

        //2.从kafka获取topic数据
        String topic = "ods_base_db_m";
        String group = "test_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, group);
        DataStreamSource<String> jsonStrDs = env.addSource(kafkaSource);

        jsonStrDs.print("转换前-->");
        //3.对数据进行json转换
        SingleOutputStreamOperator<JSONObject> jsonObjDs = jsonStrDs.map(jsonObj ->{
            System.out.println(4/0);
            JSONObject jsonObject = JSON.parseObject(jsonObj);
            return jsonObject;
        });

        jsonObjDs.print("转换后-->");

        try {
            env.execute("base db task");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
