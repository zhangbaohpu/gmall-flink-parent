package com.zhangbao.gmall.realtime.app.dwm;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.db.sql.Order;
import com.alibaba.fastjson.JSONObject;
import com.zhangbao.gmall.realtime.app.func.DimAsyncFunction;
import com.zhangbao.gmall.realtime.bean.OrderDetail;
import com.zhangbao.gmall.realtime.bean.OrderInfo;
import com.zhangbao.gmall.realtime.bean.OrderWide;
import com.zhangbao.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;


/**
 * @author zhangbao
 * @date 2021/10/25 19:58
 * @desc
 * 启动服务
 *      zk > kf > maxwell > hdfs > hbase > baseDbTask > OrderWideApp > mysql配置表
 * 业务流程
 *      模拟生成数据
 *      maxwell监控mysql数据
 *      kafka接收maxwell发送的数据，放入ODS层（ods_base_db_m）
 *      baseDbTask消费kafka的主题数据并进行分流
 *          从mysql读取配置表
 *          将配置缓存到map集合中
 *          检查phoenix（hbase的皮肤）是否存在表
 *          对数据表进行分流发送到不同dwd层主题
 */
public class OrderWideApp {
    public static void main(String[] args) {
        //webui模式，需要添加pom依赖
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.createLocalEnvironment();
        //设置并行度
        env.setParallelism(4);
        //设置检查点
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9000/gmall/flink/checkpoint/uniqueVisit"));
//        //指定哪个用户读取hdfs文件
//        System.setProperty("HADOOP_USER_NAME","zhangbao");


        //从kafka的dwd主题获取订单和订单详情
        String orderInfoTopic = "dwd_order_info";
        String orderDetailTopic = "dwd_order_detail";
        String orderWideTopic = "dwm_order_wide";
        String orderWideGroup = "order_wide_group";

        //订单数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoTopic, orderWideGroup);
        DataStreamSource<String> orderInfoDs = env.addSource(orderInfoSource);

        //订单详情数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailTopic, orderWideGroup);
        DataStreamSource<String> orderDetailDs = env.addSource(orderDetailSource);

        //对订单数据进行转换
        SingleOutputStreamOperator<OrderInfo> orderInfoObjDs = orderInfoDs.map(new RichMapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String jsonStr) throws Exception {
                System.out.println("order info str >>> "+jsonStr);
                OrderInfo orderInfo = JSONObject.parseObject(jsonStr, OrderInfo.class);
                DateTime createTime = DateUtil.parse(orderInfo.getCreate_time(), "yyyy-MM-dd HH:mm:ss");
                orderInfo.setCreate_ts(createTime.getTime());
                return orderInfo;
            }
        });

        //对订单明细数据进行转换
        SingleOutputStreamOperator<OrderDetail> orderDetailObjDs = orderDetailDs.map(new RichMapFunction<String, OrderDetail>() {
            @Override
            public OrderDetail map(String jsonStr) throws Exception {
                System.out.println("order detail str >>> "+jsonStr);
                OrderDetail orderDetail = JSONObject.parseObject(jsonStr, OrderDetail.class);
                DateTime createTime = DateUtil.parse(orderDetail.getCreate_time(), "yyyy-MM-dd HH:mm:ss");
                orderDetail.setCreate_ts(createTime.getTime());
                return orderDetail;
            }
        });

//        orderInfoObjDs.print("order info >>>");
//        orderDetailObjDs.print("order detail >>>");

        //指定事件时间字段
        //订单事件时间字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDs = orderInfoObjDs.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long l) {
                                return orderInfo.getCreate_ts();
                            }
                        })
        );
        //订单明细指定事件事件字段
        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDs = orderDetailObjDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        })
        );

        //分组
        KeyedStream<OrderInfo, Long> orderInfoKeysDs = orderInfoWithTsDs.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeysDs = orderDetailWithTsDs.keyBy(OrderDetail::getOrder_id);

        /**
         * interval-join
         * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/operators/joining/#interval-join
         */
        SingleOutputStreamOperator<OrderWide> orderWideDs = orderInfoKeysDs.intervalJoin(orderDetailKeysDs)
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
//        orderWideDs.print("order wide ds >>>");

        /**
         * 关联用户维度数据
         * flink异步查询
         * https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/datastream/operators/asyncio/#%e5%bc%82%e6%ad%a5-io-api
         */
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDs = AsyncDataStream.unorderedWait(orderWideDs, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
            @Override
            public String getKey(OrderWide obj) {
                return obj.getUser_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) {
                Date birthday = dimInfo.getDate("BIRTHDAY");
                Long age = DateUtil.betweenYear(birthday, new Date(), false);
                orderWide.setUser_age(age.intValue());
                orderWide.setUser_gender(dimInfo.getString("GENDER"));
            }
        }, 60, TimeUnit.SECONDS);

//        orderWideWithUserDs.print("order wide with users >>>");

        /**
         * 关联省份维度
         * 以上一个流为基础，关联省份数据
         */
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDs = AsyncDataStream.unorderedWait(orderWideWithUserDs,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                }, 60, TimeUnit.SECONDS);
//        orderWideWithProvinceDs.print("order wide with province>>>");

        /**
         * 关联sku数据
         */
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDs = AsyncDataStream.unorderedWait(orderWideWithProvinceDs,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
                        orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                        orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(dimInfo.getLong("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        /**
         * 关联spu数据
         */
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDs = AsyncDataStream.unorderedWait(orderWideWithSkuDs, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getSpu_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) {
                orderWide.setSpu_name(dimInfo.getString("SPU_NAME"));

            }
        }, 60, TimeUnit.SECONDS);

        /**
         * 关联品类数据
         */

        SingleOutputStreamOperator<OrderWide> orderWideWithCategoryDs = AsyncDataStream.unorderedWait(orderWideWithSpuDs, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getCategory3_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) {
                orderWide.setCategory3_name(dimInfo.getString("NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        /**
         * 关联品牌数据
         */

        SingleOutputStreamOperator<OrderWide> orderWideWithTmDs = AsyncDataStream.unorderedWait(orderWideWithCategoryDs, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getTm_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) {
                orderWide.setTm_name(dimInfo.getString("TM_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        orderWideWithTmDs.print("order wide with sku_spu_category_tm >>> ");


        try {
            env.execute("order wide task");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
