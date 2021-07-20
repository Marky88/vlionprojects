package com.vlion.task;

import com.vlion.bean.Consumer;
import com.vlion.bean.IntendUser;
import com.vlion.sink.MysqlSinkAgg;
import com.vlion.utils.PropertiesUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Currency;
import java.util.Date;
import java.util.Properties;

/**
 * @description:
 * @author: malichun
 * @time: 2021/7/20/0020 13:39
 */
public class BlackListRealTimeTask {
    public static void main(String[] args) throws Exception {
        //1.0kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", PropertiesUtils.getString("kafka.bootstrap.servers"));
        properties.setProperty("group.id", PropertiesUtils.getString("kafka.group.id") + "_2"); // 不同的消费者组
        properties.setProperty("auto.offset.reset", "latest");


        // 连接到Redis的配置
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
            .setHost(PropertiesUtils.getString("redis_host"))
            .setPort(6379)
            .setMaxTotal(100)
            .setTimeout(1000 * 10)
            .build();


        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 1.状态后端配置
        env.setStateBackend(new FsStateBackend(PropertiesUtils.getString("flink.checkpoint.dir") + "_2"));
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
        WatermarkStrategy<Tuple2<String, Long>> wms = WatermarkStrategy
            .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                @Override
                public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
//                        System.out.println("watermark:\t"+element);
                    return element.f1 * 1000L;
                }
            });

        // 获取kafka源,分配watermarks
        env
            .addSource(new FlinkKafkaConsumer<>(PropertiesUtils.getString("kafka.topic"), new SimpleStringSchema(), properties))
            .filter(str -> str.startsWith("316\t")) // consumer
            .flatMap(new RichFlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                    String[] arr = value.split("\t");
                    if (arr.length >= 25) {
                        out.collect(Tuple2.of(arr[24], Long.valueOf(arr[1]))); // (ip, 时间戳)
                    }
                }
            })
            .assignTimestampsAndWatermarks(wms)
            .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                @Override
                public String getKey(Tuple2<String, Long> value) throws Exception {
                    return value.f0;
                }
            })
            .window(SlidingEventTimeWindows.of(Time.days(7), Time.days(1))) // 窗口7天,每天更新
            .allowedLateness(Time.seconds(10))
            // 使用process 实现黑名单过滤
            .aggregate( new IpCountAgg(),new IpCountResult())
            .keyBy(t -> t.f2) // 根据窗口分组
            .process(new FilterBlackListIP(4))
            .addSink(new RedisSink<Tuple2<String,Long>>(redisConfig, new RedisMapper<Tuple2<String,Long>>() {
                @Override
                public RedisCommandDescription getCommandDescription() {
                    return new RedisCommandDescription(RedisCommand.SET);
                }

                @Override
                public String getKeyFromData(Tuple2<String, Long> data) {
                    return "bl:ip:"+data.f0;
                }

                @Override
                public String getValueFromData(Tuple2<String, Long> data) {
                    return data.f1.toString();
                }
            })); // 输出到redis


        env.execute("telecom blackList realtime task");

    }

    /**
     * IN, ACC, OUT
     */
    public static class IpCountAgg implements AggregateFunction<Tuple2<String,Long>,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> value, Long accumulator) {
            return accumulator +1 ;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    //<IN, OUT, KEY, W extends Window> // IN输入是增量聚合的输出,OUT是 <ip,count,windowEnd>
    public static class IpCountResult extends ProcessWindowFunction<Long,Tuple3<String,Long,Long>,String,TimeWindow>{

        @Override
        public void process(String ip, Context context, Iterable<Long> elements, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            long windowEnd = context.window().getEnd();
            Long count = elements.iterator().next();
            out.collect(Tuple3.of(ip,count,windowEnd));
        }
    }

    /**
     * 实现自定义处理函数
     * <K, I, O>
     */
    public static class  FilterBlackListIP extends KeyedProcessFunction<Long,Tuple3<String,Long,Long>,Tuple2<String,Long>>{
        private long threashold;

        public FilterBlackListIP(long threashold) {
            this.threashold = threashold;
        }

        private ValueState<Long> windowEnd;
        private ListState<Tuple3<String, Long, Long>> datas;

        @Override
        public void open(Configuration parameters) throws Exception {
            datas = getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String, Long, Long>>("datas", TypeInformation.of(new TypeHint<Tuple3<String, Long, Long>>() {})));
            windowEnd = getRuntimeContext().getState(new ValueStateDescriptor<Long>("windowEnd", Long.class));
        }

        @Override
        public void processElement(Tuple3<String, Long, Long> value, Context ctx, Collector<Tuple2<String,Long>> out) throws Exception {
            // 存数据
            datas.add(value);
            // 注册定时器
            if(windowEnd.value() == null){
                ctx.timerService().registerEventTimeTimer(value.f2+10L);
                windowEnd.update(value.f2);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String,Long>> out) throws Exception {
            ArrayList<Tuple3<String,Long,Long>> result = new ArrayList<>();
            for (Tuple3<String, Long, Long> t : datas.get()) {
                result.add(t);
            }

            // 清空状态
            windowEnd.clear();
            datas.clear();
            //过滤,找出超过阈值的,发送到主流
            result.stream().filter(t -> t.f1 >= threashold).map(t -> Tuple2.of(t.f0,t.f1)).forEach(out::collect); // ip,次数
        }
    }
}

