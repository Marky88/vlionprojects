package com.vlion.task;

import com.vlion.bean.Consumer;
import com.vlion.bean.OrderDetail;
import com.vlion.sink.MysqlSinkOneByOne;
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
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;

/**
 * 每个导入到mysql
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
//                System.out.println("输入: "+value);
                if (value != null) {
                    String[] arr = value.split("\t", -1);

                    if (arr[0].equals("316") && arr.length == 18) { // 下单用户 consumer
                        String orderId = arr[3];
                        String templateId = arr[4];
                        String cartNo = arr[5];
                        String buyerName = arr[6];
                        String mobilePhone = arr[7];
                        String receiverProv = arr[8];
                        String receiverCity = arr[9];
                        String receiverDistrict = arr[10];
                        String receiverAddress = arr[11];
                        String planId = arr[12];
                        String creativeId = arr[13];
                        String comboType = arr[17] ;// 套餐类型
                        String isChooseNum = null; // 是否选号,预留
                        Long time = null;
                        if(arr[1]!=null && !arr[1].equals("")) time=Long.parseLong(arr[1]);// 时间戳

                        // 获取
//                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//                        Date date = sdf.parse(sdf.format(time * 1000));
                        Consumer consumer = new Consumer();
                        if(!(orderId== null || orderId.trim().equals(""))) consumer.setOrderId(orderId);
                        if(!(templateId== null || templateId.trim().equals(""))) consumer.setTemplateId(templateId);
                        if(!(cartNo== null || cartNo.trim().equals(""))) consumer.setCartNo(cartNo);
                        if(!(buyerName== null || buyerName.trim().equals(""))) consumer.setBuyerName(buyerName);
                        if(!(mobilePhone== null || mobilePhone.trim().equals(""))) consumer.setMobilePhone(mobilePhone);
                        if(!(receiverProv== null || receiverProv.trim().equals(""))) consumer.setReceiverProv(receiverProv);
                        if(!(receiverCity== null || receiverCity.trim().equals(""))) consumer.setReceiverCity(receiverCity);
                        if(!(receiverDistrict== null || receiverDistrict.trim().equals(""))) consumer.setReceiverDistrict(receiverDistrict);
                        if(!(receiverAddress== null || receiverAddress.trim().equals(""))) consumer.setReceiverAddress(receiverAddress);
                        if(!(planId== null || planId.trim().equals(""))) consumer.setPlanId(planId);
                        if(!(creativeId== null || creativeId.trim().equals(""))) consumer.setCreativeId(creativeId);
                        if(!(comboType == null || comboType.trim().equals(""))) consumer.setComboType(comboType);
                        if(time != null) consumer.setTime(time);
                        if(time!=null ) consumer.setDate(new Timestamp(time * 1000)); // 日期
//                        System.out.println("时间戳:"+ new Timestamp(time * 1000));

                        return Tuple2.of(arr[0], consumer);
                    } else if (arr[0].equals("317") && arr.length == 14) { // 订单详情  order_details
                        //订单详情表，新增了这3个字段：
                        //当other_status=“AC002”，取日志里面的时间戳字段更新active_time；
                        //当is_last_invest=“1”，取日志里面的时间戳字段更新last_invest_time；
                        //当is_invest=“1”，取日志里面的时间戳字段更新invest_time；
                        String time = null;
                        if(arr[1]!=null && !arr[1].trim().equals("")) time = new Timestamp(Long.parseLong(arr[1])*1000).toString(); // 时间
                        String orderId = arr[2];
                        String orderStatus = arr[3];
                        String otherStatus = arr[4];
                        String activeTime=null;
                        if(otherStatus.equals("AC002")){
                            activeTime=time;
                        }

                        String sendNo = arr[5]; // 物流单号

                        String logisticsName = arr[6]; // 物流公司
                        String logisticsStatus = arr[7]; // 物流状态

                        String orderStatusDesc = arr[9]; // 订单状态/做废原因

                        String isLastInvest = arr[10];  //激活后充值
                        String lastINvestTime = null;
                        if(isLastInvest.equals("1")){
                            lastINvestTime=time;
                        }
                        String isInvest = arr[11]; // 激活前充值
                        String investTime = null;
                        if("1".equals(isInvest)){
                            investTime = time;
                        }
                        String etype = arr[12]; // 头条转化类型
                        OrderDetail orderDetail = new OrderDetail();
                        if(orderId !=null && !"".equals(orderId.trim())) orderDetail.setOrderId(orderId);
                        if(orderStatus !=null && !"".equals(orderStatus.trim())) orderDetail.setOrderStatus(orderStatus);
                        if(! "".equals(otherStatus.trim())) orderDetail.setOtherStatus(otherStatus);
                        if(activeTime != null && ! "".equals(activeTime)) orderDetail.setActiveTime(activeTime);
                        if(sendNo != null && ! "".equals(sendNo)) orderDetail.setSendNo(sendNo);
                        if(logisticsName != null && ! "".equals(logisticsName)) orderDetail.setLogisticsName(logisticsName);
                        if(logisticsStatus != null && ! "".equals(logisticsStatus)) orderDetail.setLogisticsStatus(logisticsStatus);
                        if(orderStatusDesc != null && ! "".equals(orderStatusDesc)) orderDetail.setOrderStatus(orderStatusDesc);
                        if( ! "".equals(isLastInvest)) orderDetail.setIsLastInvest(isLastInvest);
                        if(lastINvestTime != null && ! "".equals(lastINvestTime)) orderDetail.setLastInvestTime(lastINvestTime);
                        if(isInvest != null && ! "".equals(isInvest)) orderDetail.setIsInvest(isInvest);
                        if(investTime != null && ! "".equals(investTime)) orderDetail.setInvestTime(investTime);
                        if(etype != null && ! "".equals(etype)) orderDetail.setEtype(etype);
//                        if(comboType != null && ! "".equals(comboType)) orderDetail.setComboType(comboType);
//                        if(isChooseNum != null && ! "".equals(isChooseNum)) orderDetail.setIsChooseNum(isChooseNum);
//                        System.out.println("orderDetail:\t"+orderDetail);
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

        logTypeAndGENERIC.addSink(new MysqlSinkOneByOne()).name("mysql sink");
//        logTypeAndGENERIC.print();


        env.execute("telecom file to mysql ");

    }
}
