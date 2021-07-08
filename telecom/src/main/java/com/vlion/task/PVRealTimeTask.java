package com.vlion.task;

import com.vlion.bean.IntendUser;
import com.vlion.sink.MysqlSinkAgg;
import com.vlion.utils.PropertiesUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple3;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;

/**
 * （每小时+每个状态码+h5模板）统计一条数据入库
 * intend_user
 */
public class PVRealTimeTask {
    public static void main(String[] args) throws Exception {

        //1.0kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", PropertiesUtils.getString("kafka.bootstrap.servers"));
        properties.setProperty("group.id", PropertiesUtils.getString("kafka.group.id") + "_1"); // 不同的消费者组
        properties.setProperty("auto.offset.reset", "latest");

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.状态后端配置
        env.setStateBackend(new FsStateBackend(PropertiesUtils.getString("flink.checkpoint.dir") + "_1"));
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
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.minutes(10), org.apache.flink.api.common.time.Time.minutes(1)));


        // 创建watermarkStrategy
        WatermarkStrategy<IntendUser> wms = WatermarkStrategy
            .<IntendUser>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner(new SerializableTimestampAssigner<IntendUser>() {
                @Override
                public long extractTimestamp(IntendUser element, long recordTimestamp) {
//                        System.out.println("watermark:\t"+element);
                    return Long.parseLong(element.getTime()) * 1000L;
                }
            });

        // 获取kafka源
        DataStreamSource<String> kafkaSource = env
            .addSource(new FlinkKafkaConsumer<>(PropertiesUtils.getString("kafka.topic"), new SimpleStringSchema(), properties));


        kafkaSource.filter(str -> str.startsWith("314\t")) // 意向用户
            .flatMap(new RichFlatMapFunction<String, IntendUser>() {

                @Override
                public void flatMap(String line, Collector<IntendUser> out) throws Exception {
//                        System.out.println("输入的line:"+line);
                    String[] arr = line.split("\t", -1);
//                        System.out.println("line的长度"+arr.length);
                    if (arr.length >= 18) {
                        out.collect(new IntendUser(arr[4], // 入口模版
                            arr[2], // 状态码
                            arr[3], // 错误原因
                            arr[1] // 时间戳
                        ));
                    }
                }
            })
            .assignTimestampsAndWatermarks(wms) // 添加watermark
            .map(new RichMapFunction<IntendUser, Tuple2<Tuple4<String, String, String, String>, Long>>() {
                @Override
                public Tuple2<Tuple4<String, String, String, String>, Long> map(IntendUser intenduser) throws Exception {
//                        System.out.println("intenduser:\t"+intenduser);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
                    String format = sdf.format(new Date(Long.parseLong(intenduser.getTime()) * 1000L));
                    return Tuple2.of(Tuple4.of(intenduser.getTemplateId(), intenduser.getCode(), intenduser.getMsg(), format), 1L); // templateId,code,msg作为key
                }
            })
//                .returns(Types.TUPLE(Types.TUPLE(Types.STRING,Types.STRING,Types.STRING),Types.LONG)) // 使用tuple类型,方便后面求和
            .keyBy(new KeySelector<Tuple2<Tuple4<String, String, String, String>, Long>, Tuple4<String, String, String, String>>() {
                @Override
                public Tuple4<String, String, String, String> getKey(Tuple2<Tuple4<String, String, String, String>, Long> value) throws Exception {
                    return value.f0;
                }
            })
            // 分配窗口
            .window(TumblingEventTimeWindows.of(Time.minutes(60)))
            .allowedLateness(Time.minutes(2))
            .sum(1) // 求和
//                .print();
            .addSink(new MysqlSinkAgg());

        env.execute("telecom pv realtime task");


    }
}
