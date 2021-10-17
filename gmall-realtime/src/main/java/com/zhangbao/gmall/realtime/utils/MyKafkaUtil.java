package com.zhangbao.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author: zhangbao
 * @date: 2021/6/18 23:41
 * @desc:
 **/
public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "hadoop101:9092,hadoop102:9092,hadoop103:9092";
    private static final String DEFAULT_TOPIC = "default_data";
    /**
     * kafka消费者
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String group){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(),props);
    }

    /**
     *kafka生产者
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(KAFKA_SERVER, topic, new SimpleStringSchema());
    }

    /**
     * 动态生产到不同的topic，如果不传topic，则自动生产到默认的topic
     * @param T  序列化后的数据，可指定topic
     */
    public static <T> FlinkKafkaProducer<T> getKafkaBySchema(KafkaSerializationSchema<T> T){
        Properties pros = new Properties();
        pros.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        //设置生产数据的超时时间
        pros.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,T,pros,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

}
