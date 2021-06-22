package com.vlion.task;

import com.vlion.bean.Consumer;
import com.vlion.bean.OrderDetail;
import com.vlion.sink.SinkToMySql;
import com.vlion.utils.PropertiesUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.PrintStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;

/**
 * @description:
 * @author: malichun
 * @time: 2021/6/18/0018 10:07
 */
public class RealTimeTask {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String runType = params.get("runtype");
        System.out.println("runType:" + runType);


        //1.0kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", PropertiesUtils.getString("kafka.bootstrap.servers"));
        properties.setProperty("group.id", PropertiesUtils.getString("kafka.group.id"));
        properties.setProperty("auto.offset.reset", "latest");

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend(PropertiesUtils.getString("flink.checkpoint.dir")));

        DataStreamSource<String> kafkaSource = env
                .addSource(new FlinkKafkaConsumer<>(PropertiesUtils.getString("kafka.topic"), new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<Tuple2<String, Object>> logTypeAndGENERIC = kafkaSource.map(new RichMapFunction<String, Tuple2<String, Object>>() {
            @Override
            public Tuple2<String, Object> map(String value) throws Exception {
                if (value != null) {
                    String[] arr = value.split("\t", -1);

                    if (arr[0].equals("316") && arr.length == 16) { // 下单用户 consumer
                        String orderId = arr[3];
                        String templateId = arr[4];
                        String cartNo = arr[5];
                        String buyerName = arr[6];
                        String mobilePhone = arr[7];
                        String receiverProv = arr[8];
                        String receiverCity = arr[9];
                        String receiverDistrict = arr[10];
                        String receiverAddress = arr[11];
                        long time = Long.parseLong(arr[1]);// 时间戳

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                        Date date = sdf.parse(sdf.format(time * 1000));

                        Consumer consumer = new Consumer(orderId,
                                templateId,
                                cartNo,
                                buyerName,
                                mobilePhone,
                                receiverProv,
                                receiverCity,
                                receiverDistrict,
                                receiverAddress,
                                time, // 时间戳
                                new Timestamp(date.getTime()) //日期
                        );
                        return Tuple2.of(arr[0], consumer);
                    } else if (arr[0].equals("317") && arr.length == 14) { // 订单详情  order_details
                        String orderId = arr[2];
                        String orderStatus = arr[3];
                        String otherStatus = arr[4];
                        String sendNo = arr[5]; // 物流单号

                        String logisticsName = arr[6]; // 物流公司
                        String logisticsStatus = arr[7]; // 物流状态

                        String orderStatusDesc = arr[9]; // 订单状态/做废原因

                        String isLastInvest = arr[10];  //激活后充值
                        String isInvest = arr[11]; // 激活前充值
                        String etype = arr[12]; // 头条转化类型
                        OrderDetail orderDetail = new OrderDetail(
                                orderId,
                                orderStatus,
                                otherStatus,
                                sendNo,
                                logisticsName,
                                logisticsStatus,
                                orderStatusDesc,
                                isLastInvest,
                                isInvest,
                                etype
                        );
                        return Tuple2.of("317", orderDetail);
                    } else {
                        return null;
                    }
                } else {
                    return null;
                }
            }
        }).filter(Objects::nonNull)
                .returns(Types.TUPLE(Types.STRING, Types.GENERIC(Object.class)));

        logTypeAndGENERIC.addSink(new SinkToMySql()).name("mysql sink");

        env.execute("telecom file to mysql ");

    }
}
