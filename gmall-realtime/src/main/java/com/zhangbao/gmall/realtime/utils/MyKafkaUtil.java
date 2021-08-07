package com.zhangbao.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author: zhangbao
 * @date: 2021/6/18 23:41
 * @desc:
 **/
public class MyKafkaUtil {
    private static String kafka_host = "hadoop101:9092,hadoop102:9092,hadoop103:9092";

    /**
     * kafka消费者
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String group){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafka_host);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(),props);
    }

    /**
     *kafka生产者
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(kafka_host, topic, new SimpleStringSchema());
    }

}
