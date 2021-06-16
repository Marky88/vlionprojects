package com.vlion.sparkstreaming.stopGracefully

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * 对接 socket
 */
object StreamWordCount {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("StreamWordCount").setMaster("local[*]")

        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")

        //checkpointPath是hdfs的路径
        val ssc: StreamingContext = StreamingContext.getActiveOrCreate("/root/kafka/ck", () => createSSC())

        //        ssc.checkpoint("/root/kafka/ck")

        //通过监控端口创建DStream
        val lineStreams = ssc.socketTextStream("www.bigdata05.com", 9999)

        //将每一行数据切分,形成一个单词
        val wordStreams = lineStreams.flatMap(_.split(" "))

        //将单词映射成元组
        val wordAndOneStreams = wordStreams.map((_, 1))

        //将相同的单词次数做统计
        val wordAndCountStreams = wordAndOneStreams.reduceByKey(_ + _)

        //打印
        wordAndCountStreams.print()

        //优雅的关闭
        val fs: FileSystem = FileSystem.get(new URI("hdfs://www.bigdata02.com:8020"), new Configuration(), "root")
        new Thread(){
        while (true) {
            try
                Thread.sleep(5000)
            catch {
                case e: InterruptedException =>
                    e.printStackTrace()
            }
            val state: StreamingContextState = ssc.getState

            val bool: Boolean = fs.exists(new Path("hdfs://www.bigdata02.com:8020/tmp/test/stop"))

            if (bool) {
                if (state == StreamingContextState.ACTIVE) {
                    ssc.stop(stopSparkContext = true, stopGracefully = true)
                    sys.exit(0)
                }
            }
        }
    }.start


        //启动SparkStreamingContext
        ssc.start()




        ssc.awaitTermination()



    }



    def createSSC(): _root_.org.apache.spark.streaming.StreamingContext = {

        val update: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], status: Option[Int]) => {

            //当前批次内容的计算
            val sum: Int = values.sum

            //取出状态信息中上一次状态
            val lastStatu: Int = status.getOrElse(0)

            Some(sum + lastStatu)
        }

        val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkTest")

        //设置优雅的关闭
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

        val ssc = new StreamingContext(sparkConf, Seconds(5))

        ssc.checkpoint("./ck")

        val line: ReceiverInputDStream[String] = ssc.socketTextStream("linux1", 9999)

        val word: DStream[String] = line.flatMap(_.split(" "))

        val wordAndOne: DStream[(String, Int)] = word.map((_, 1))

        val wordAndCount: DStream[(String, Int)] = wordAndOne.updateStateByKey(update)

        wordAndCount.print()

        ssc
    }
}
