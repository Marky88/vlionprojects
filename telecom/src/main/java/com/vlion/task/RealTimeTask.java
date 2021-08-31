package com.vlion.task;

import com.vlion.bean.Consumer;
import com.vlion.bean.OrderDetail;
import com.vlion.sink.MysqlSinkOneByOne;
import com.vlion.utils.PropertiesUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import javax.jdo.annotations.Order;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;

/**
 * 每个导入到mysql
 * @author malichun
 * @time: 2021/6/9/0009 15:54
 * @
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

        // 1.状态后端配置
        env.setStateBackend(new FsStateBackend(PropertiesUtils.getString("flink.checkpoint.dir")));
        // 2.检查点配置
        env.enableCheckpointing(5000);
          // 高级选项
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //Checkpoint的处理超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 最大允许同时处理几个Checkpoint(比如上一个处理到一半，这里又收到一个待处理的Checkpoint事件)
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 与上面setMaxConcurrentCheckpoints(2) 冲突，这个时间间隔是 当前checkpoint的处理完成时间与接收最新一个checkpoint之间的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        //最多能容忍几次checkpoint处理失败（默认0，即checkpoint处理失败，就当作程序执行异常）
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
        // 开启在 job 中止后仍然保留的 externalized checkpoints  // 作业取消时外部化检查点的清理行为, 在作业取消时保留外部检查点。
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 3.重启策略配置
        // 固定延迟重启(最多尝试3次，每次间隔10s)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 失败率重启(在10分钟内最多尝试3次，每次至少间隔1分钟)
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));




        DataStreamSource<String> kafkaSource = env
                .addSource(new FlinkKafkaConsumer<>(PropertiesUtils.getString("kafka.topic"), new SimpleStringSchema(), properties));
//                  .addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));

        // 这边使用处理时间吧
        SingleOutputStreamOperator<Tuple2<String, Object>> logTypeAndGENERIC = kafkaSource
            .map(new RichMapFunction<String, Tuple2<String, Object>>() {
            @Override
            public Tuple2<String, Object> map(String value) throws Exception {
//                System.out.println("输入: "+value);
//                System.out.println("输入数据的长度:"+value.split("\t",-1).length);
                if (value != null) {
                    String[] arr = value.split("\t", -1); // 要加-1
//                    System.out.println("长度:"+ arr.length);
                    if (arr[0].equals("316") && arr.length >= 44 && !arr[3].equals("")) { // 下单用户 consumer ,一定要有orderId
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
                        String comboType = arr[17];// 套餐类型
                        String isChooseNum = null; // 是否选号,预留
                        Long time = null;
                        if (arr[1] != null && !arr[1].equals("")) time = Long.parseLong(arr[1]);// 时间戳
                        String orderMobilePhone = arr[18]; // 下单号码
                        String channelId = arr[16]; // 渠道ID
                        String sourceType = arr[19];// 来源方式打标说明
                        String flowType = arr[20]; // 引流平台打标说明
                        String pid = arr[22]; //一级代理
                        String eid = arr[23]; //一级代理
                        String ip = arr[24]; //用户ip
                        String carrier = arr[33]; // 运营商
                        String gdtNoWXClkId = arr[37]; //gd5点击id(非微信)
                        String vlionOrderId = arr[43]; // vlion订单id
                        // 获取
//                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//                        Date date = sdf.parse(sdf.format(time * 1000));
                        Consumer consumer = new Consumer();
                        if (!(orderId == null || orderId.trim().equals(""))) consumer.setOrderId(orderId);
                        if (!(templateId == null || templateId.trim().equals(""))) consumer.setTemplateId(templateId);
                        if (!(cartNo == null || cartNo.trim().equals(""))) consumer.setCartNo(cartNo);
                        if (!(buyerName == null || buyerName.trim().equals(""))) consumer.setBuyerName(buyerName);
                        if (!(mobilePhone == null || mobilePhone.trim().equals("")))
                            consumer.setMobilePhone(mobilePhone);
                        if (!(receiverProv == null || receiverProv.trim().equals("")))
                            consumer.setReceiverProv(receiverProv);
                        if (!(receiverCity == null || receiverCity.trim().equals("")))
                            consumer.setReceiverCity(receiverCity);
                        if (!(receiverDistrict == null || receiverDistrict.trim().equals("")))
                            consumer.setReceiverDistrict(receiverDistrict);
                        if (!(receiverAddress == null || receiverAddress.trim().equals("")))
                            consumer.setReceiverAddress(receiverAddress);
                        if (!(planId == null || planId.trim().equals(""))) consumer.setPlanId(planId);
                        if (!(creativeId == null || creativeId.trim().equals(""))) consumer.setCreativeId(creativeId);
                        if (!(comboType == null || comboType.trim().equals(""))) consumer.setComboType(comboType);
                        if (time != null) consumer.setTime(time);
                        if (time != null) consumer.setDate(new Timestamp(time * 1000)); // 日期
                        if(orderMobilePhone !=null && !orderMobilePhone.trim().equals("")) consumer.setOrderMobilePhone(orderMobilePhone);
                        if(channelId !=null && !channelId.trim().equals("")) consumer.setChannelId(channelId);
//                        System.out.println("时间戳:"+ new Timestamp(time * 1000));
                        if(sourceType != null && !sourceType.trim().equals("")) consumer.setSourceType(sourceType);
                        if(flowType != null && !flowType.trim().equals("")) consumer.setFlowType(flowType);
                        if(pid != null && !pid.trim().equals("")) consumer.setPid(pid);
                        if(eid != null && !eid.trim().equals("")) consumer.setEid(eid);
                        if(ip !=null && !ip.trim().equals("")) consumer.setIp(ip);
                        if(carrier != null && !carrier.trim().equals("")) consumer.setCarrier(carrier);
                        if(gdtNoWXClkId != null && !gdtNoWXClkId.trim().equals("")) consumer.setGdtNoWXClkId(gdtNoWXClkId);
                        if(vlionOrderId != null && !vlionOrderId.trim().equals("")) consumer.setVlionOrderId(vlionOrderId);
//                        System.out.println("输入的consumer:"+ consumer);
                        return Tuple2.of(arr[0], consumer);
                    } else if (arr[0].equals("317") && arr.length >= 19 && !arr[2].equals("")) { // 订单详情  order_details
                        //订单详情表，新增了这3个字段：
                        //当other_status=“AC002”，取日志里面的时间戳字段更新active_time；
                        //当is_last_invest=“1”，取日志里面的时间戳字段更新last_invest_time；
                        //当is_invest=“1”，取日志里面的时间戳字段更新invest_time；
                        String time = null;
                        if (arr[1] != null && !arr[1].trim().equals(""))
                            time = new Timestamp(Long.parseLong(arr[1]) * 1000).toString(); // 时间
                        String orderId = arr[2];
                        String orderStatus = arr[3];
                        String otherStatus = arr[4];
                        String activeTime = null;
                        String carrier = arr[18]; // 运营商

                        if (otherStatus.equals("AC002")) {
                            activeTime = time;
                        }

                        if(carrier.equals("1")){
                            activeTime = arr[17];
                        }

                        String sendNo = arr[5]; // 物流单号

                        String logisticsName = arr[6]; // 物流公司
                        String logisticsStatus = arr[7]; // 物流状态

                        String orderStatusDesc = arr[9]; // 订单状态/做废原因

                        String isLastInvest = arr[10];  //激活后充值
                        String lastINvestTime = null;
                        if (isLastInvest.equals("1")) {
                            lastINvestTime = time;
                        }
                        String isInvest = arr[11]; // 激活前充值
                        String investTime = null;
                        if ("1".equals(isInvest)) {
                            investTime = time;
                        }
                        String etype = arr[12]; // 头条转化类型
                        String statusUpdateTime = arr[16];

                        OrderDetail orderDetail = new OrderDetail();
                        if (orderId != null && !"".equals(orderId.trim())) orderDetail.setOrderId(orderId);
                        if (orderStatus != null && !"".equals(orderStatus.trim()))
                            orderDetail.setOrderStatus(orderStatus);
                        if (!"".equals(otherStatus.trim())) orderDetail.setOtherStatus(otherStatus);
                        if (activeTime != null && !"".equals(activeTime)) orderDetail.setActiveTime(activeTime);
                        if (sendNo != null && !"".equals(sendNo)) orderDetail.setSendNo(sendNo);
                        if (logisticsName != null && !"".equals(logisticsName))
                            orderDetail.setLogisticsName(logisticsName);
                        if (logisticsStatus != null && !"".equals(logisticsStatus))
                            orderDetail.setLogisticsStatus(logisticsStatus);
                        if (orderStatusDesc != null && !"".equals(orderStatusDesc))
                            orderDetail.setOrderStatus(orderStatusDesc);
                        if (!"".equals(isLastInvest)) orderDetail.setIsLastInvest(isLastInvest);
                        if (lastINvestTime != null && !"".equals(lastINvestTime))
                            orderDetail.setLastInvestTime(lastINvestTime);
                        if (isInvest != null && !"".equals(isInvest)) orderDetail.setIsInvest(isInvest);
                        if (investTime != null && !"".equals(investTime)) orderDetail.setInvestTime(investTime);
                        if (etype != null && !"".equals(etype)) orderDetail.setEtype(etype);
//                        if(comboType != null && ! "".equals(comboType)) orderDetail.setComboType(comboType);
//                        if(isChooseNum != null && ! "".equals(isChooseNum)) orderDetail.setIsChooseNum(isChooseNum);
//                        System.out.println("orderDetail:\t"+orderDetail);
                        if(logisticsStatus != null && logisticsName.equals("10")){ // 20210714新加
                            orderDetail.setSignTime(time);
                        }
                        if(statusUpdateTime != null && !statusUpdateTime.equals("")) orderDetail.setStatusUpdateTime(statusUpdateTime);
                        if(!carrier.equals("")) orderDetail.setCarrier(carrier);
                        return Tuple2.of("317", orderDetail);
                    }else if(arr[0].startsWith("319") && arr.length >= 23){ // 和316关联,取319的广告计划id和创意id
//                        if(arr[9].equals("") || !arr[17].equals("101")){ // 只保留101的
                        if(arr[9].equals("")){  // 点击id
                            return null;
                        }else{
                            if( !arr[11].equals("") || !arr[13].equals("")) { // 不全部为空则保留
                                return Tuple2.of("319", Tuple4.of(arr[9], Long.parseLong(arr[1]), arr[11], arr[13])); // 点击id,时间戳,广告计划id,广告创意id
                            }else{
                                return null;
                            }
                        }
                    } else {
                        return null;
                    }
                } else {
                    return null;
                }
            }
        })
            .filter(Objects::nonNull).returns(Types.TUPLE(Types.STRING, Types.GENERIC(Object.class)))
            .keyBy(new KeySelector<Tuple2<String,Object>,String>(){ // 根据点击id分组
                final Random random = new Random(10);
                @Override
                public String getKey(Tuple2<String, Object> value) throws Exception {
                    if(value.f1 instanceof OrderDetail){
                        return String.valueOf(random.nextInt()); // 直接随机数吧
                        //(OrderDetail)value.f1.
                    }
                    if(value.f1 instanceof Consumer){
                        Consumer consumer = (Consumer)value.f1;
                        if(consumer.getGdtNoWXClkId() == null){
                            return String.valueOf(random.nextInt());
                        }
                        return consumer.getGdtNoWXClkId(); // 根据点击id分组
                    }
                    if(value.f1 instanceof Tuple4){
                        Tuple4<String,Long,String,String> tuple4 = (Tuple4<String,Long,String,String>)value.f1; // 点击id,时间戳,广告计划id,广告创意id
                        return tuple4.f0; // 根据点击id分组
                    }
                    return String.valueOf(random.nextInt()); // 直接随机数吧
                }
            })
        .process(new KeyedProcessFunction<String, Tuple2<String, Object>, Tuple2<String, Object>>() { //K, I, O

            private ValueState<Long> timeoutTs;
            private ValueState<Consumer> consumerValueState;
            private ValueState<Tuple4<String,Long,String,String>> tupleState;

            @Override
            public void open(Configuration parameters) throws Exception {
                timeoutTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeoutTs",Long.class));
                consumerValueState = getRuntimeContext().getState(new ValueStateDescriptor<Consumer>("consumer",Consumer.class));
                tupleState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String,Long, String, String>>(
                    "tuple4",Types.TUPLE(Types.STRING,Types.LONG, Types.STRING,Types.STRING))
                );
            }

            // I, ctx, out
            @Override
            public void processElement(Tuple2<String, Object> value, Context ctx, Collector<Tuple2<String, Object>> out) throws Exception { //
                 Object obj = value.f1;
                if(obj instanceof OrderDetail){ //直接输出到下游
                    out.collect(value);
                    return; // 这边需要退出,下面不执行了
                }
                // 这是消费者数据
                if(obj instanceof Consumer){
                    Consumer consumer = (Consumer)obj;
                    if(consumer.getGdtNoWXClkId() == null || consumer.getPlanId() !=null || consumer.getCreativeId() != null){
                        out.collect(value); //直接输出
                        return;
                    }

                    // 这边需要等待
                    // 判断数据类型
                    // 316已经来了,但是319没来
                    if(tupleState.value() == null) {
                        // 319没来,把自己保存
                        consumerValueState.update(consumer);
                        // 注册定时器,等待,将时间保存
                        if(timeoutTs.value() == null){
//                            System.out.println("316来了,319没来,注册定时器:"+(ctx.timerService().currentProcessingTime() + 20 * 60 * 1000L)); //20 * 60 * 1000L
//                            ctx.timerService().registerProcessingTimeTimer(consumer.getTime() * 1000L + 20 * 60 * 1000L);
                            long timerTime = ctx.timerService().currentProcessingTime() + 20 * 60 * 1000L;
                            ctx.timerService().registerProcessingTimeTimer(timerTime);
                            timeoutTs.update(timerTime); // 等待20分钟
                        }

                    }else{
                        // 319之前已经来了
                        consumer.setPlanId(tupleState.value().f2);
                        consumer.setCreativeId(tupleState.value().f3);
//                        System.out.println("316来之前319已经到了,"+tupleState.value().f2+ "===="+tupleState.value().f3);
                        out.collect(Tuple2.of("316",consumer)); // 组合之后输出
//                        System.out.println("清空状态和定时器");
                        tupleState.clear();
                        consumerValueState.clear();
                        timeoutTs.clear();
                        if(timeoutTs.value()!=null) {
                            ctx.timerService().deleteProcessingTimeTimer(timeoutTs.value()); // 删除定时器
                        }
                    }

                }

                // 这是附加数据
                if(obj instanceof Tuple4){
                    Tuple4<String,Long,String,String> tuple4 = (Tuple4<String,Long,String,String>)obj; // 点击id,时间戳,计划id,创意id
                    if(consumerValueState.value() == null){
                        // 319来了,316没有来,把自己存起来
                        tupleState.update(tuple4);
                        // 更新等待时间
                        if(timeoutTs.value() == null){
//                            System.out.println("319来了,316没来,注册定时器:"+(ctx.timerService().currentProcessingTime() + 20 * 60 * 1000L));
//                            ctx.timerService().registerProcessingTimeTimer(tuple4.f1*1000L + 20 * 60 * 1000L);
                            long timerTime = ctx.timerService().currentProcessingTime() + 20 * 60 * 1000L;
                            ctx.timerService().registerProcessingTimeTimer(timerTime);
                            timeoutTs.update(timerTime);
                        }
                    }else{
                        Consumer consumer = consumerValueState.value();
                        consumer.setPlanId(tuple4.f2);
                        consumer.setCreativeId(tuple4.f3);
//                        System.out.println("316来之前319已经到了,plan_id:"+consumer.getPlanId()+ "====,creativeId:"+consumer.getCreativeId());
                        out.collect(Tuple2.of("316",consumer)); // 组合之后输出
//                        System.out.println("清空状态和定时器");
                        tupleState.clear();
                        consumerValueState.clear();
                        timeoutTs.clear();
                        if(timeoutTs.value()!=null) {
                            ctx.timerService().deleteProcessingTimeTimer(timeoutTs.value()); // 删除定时器
                        }
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Object>> out) throws Exception {
//                System.out.println("定时器触发....");
                if(consumerValueState.value() != null){
                    out.collect(Tuple2.of("316",consumerValueState.value()));
                }
                // 删除状态
                tupleState.clear();
                consumerValueState.clear();
                timeoutTs.clear();
            }
        });
        logTypeAndGENERIC.addSink(new MysqlSinkOneByOne()).name("mysql sink");
//        logTypeAndGENERIC.print();


        env.execute("telecom file to mysql ");

    }
}
