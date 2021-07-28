package com.advlion.www.realtime_flink

import java.sql.Timestamp
import java.util.Properties

import com.advlion.www.collaborative.Clk_log
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @description:
  * @author: malichun
  * @time: 2020/9/22/0022 17:56
  *        需求:
  *        2小时热门新闻id的统计,每5s统计一次
  *
  */
//定义输入日志
case class Clk_log2(timestamp: Long, mediaId: String, channel: String, logType: String, contentId: String, cookie: String)

//定义窗口聚合结果样例类
case class ContentViewCount(contentId: String, windowEnd: Long, count: Long)

object RealTimeAppFlink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        //从kafka读取数据
        //3.从kafka读取数据
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "www.bigdata02.com:9092,www.bigdata03.com:9092,www.bigdata04.com:9092")
        properties.setProperty("group.id", "test")
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty("auto.offset.reset","latest")

        val inputStream = env.addSource(new FlinkKafkaConsumer[String]("hippo_clk", new SimpleStringSchema(), properties))

        val dataStream = inputStream
            .map(record => if (record == null) null else record.split("\t"))
            .filter(arr => arr != null && arr.length > 21)
            .map(arr => {
                //  频道,//类型,1新闻 2广告,    //新闻id或广告位置 //cookie
                //channel:String,logType:String,contentId:String,cookie:String
                Clk_log2(arr(1).toLong,arr(3).trim, arr(15).trim, arr(18).trim, arr(19).trim, arr(20).trim)
            })
            .filter(
                c => (c != null) &&
                    c.logType == "1" &&
                    (c.contentId != null || c.contentId != "") &&
                    c.contentId.length > 32 &&
                    c.contentId.contains("_") &&
                    c.channel.matches("[\\u4e00-\\u9fa5]+") //频道为中文
            ) //过滤内容ID为空的和前提 logType为新闻的
//            .assignAscendingTimestamps(_.timestamp * 1000L)
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Clk_log2](Time.seconds(2)) {
                override def extractTimestamp(t: Clk_log2): Long = {
                    t.timestamp * 1000L
                }
            })
        //得到窗口聚合结果
        val aggStream = dataStream
            .keyBy("contentId")
            .timeWindow(Time.hours(2),Time.seconds(5))
            .aggregate(new CountAgg(),new ContentViewWindowResult())

        val resultStream = aggStream
            .keyBy("windowEnd")
            .process(new TopNHotItems(10))

        resultStream.print("")

        env.execute("hot items")
    }
}

class CountAgg extends AggregateFunction[Clk_log2, Long, Long] { //public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable {

    override def createAccumulator(): Long = 0L

    // 每来一条数据调用一次add，count值加一
    override def add(value: Clk_log2, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
}

class ContentViewWindowResult extends WindowFunction[Long,ContentViewCount,Tuple,TimeWindow] { //trait WindowFunction[IN, OUT, KEY, W <: Window] extends Function with Serializable {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ContentViewCount]): Unit = {
        val contentId = key.asInstanceOf[Tuple1[String]].f0
        val windowEnd = window.getEnd
        val count = input.iterator.next()
        out.collect(ContentViewCount(contentId,windowEnd,count))

    }
}



class TopNHotItems(topN:Int) extends KeyedProcessFunction[Tuple, ContentViewCount, String] { //public abstract class KeyedProcessFunction<K, I, O> extends AbstractRichFunction {
    //先定义状态:ListState
    var itemViewCountListState: ListState[ContentViewCount] = _

    override def open(parameters: Configuration): Unit = {
        itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor("itemViewCount", classOf[ContentViewCount]))
    }

    override def processElement(value: ContentViewCount, ctx: KeyedProcessFunction[Tuple, ContentViewCount, String]#Context, out: Collector[String]): Unit = {
        //每来一条数据,直接加入ListState
        itemViewCountListState.add(value)
        //注册一个windowEnd + 1 之后出发的定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

    }

    //定时器触发,可以认为所有窗口统计结果都已到齐,可以排序输出了
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ContentViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        // 为了方便排序，另外定义一个ListBuffer，保存ListState里面的所有数据
        val allItemViewCounts: ListBuffer[ContentViewCount] = ListBuffer()
        val iter = itemViewCountListState.get().iterator()
        while(iter.hasNext){
            allItemViewCounts += iter.next()
        }

        // 清空状态
        itemViewCountListState.clear()

        //按照count大小排序,取前n个
        val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topN)

        // 将排名信息格式化成String，便于打印输出可视化展示
        val result: StringBuilder = new StringBuilder
        result.append("窗口结束时间：").append( new Timestamp(timestamp - 1) ).append("\n")

        // 遍历结果列表中的每个ItemViewCount，输出到一行
        for( i <- sortedItemViewCounts.indices ){
            val currentItemViewCount = sortedItemViewCounts(i)
            result.append("NO").append(i + 1).append(": \t")
                .append("商品ID = ").append(currentItemViewCount.contentId).append("\t")
                .append("热门度 = ").append(currentItemViewCount.count).append("\n")
        }

        result.append("\n==================================\n\n")

        out.collect(result.toString())

    }
}