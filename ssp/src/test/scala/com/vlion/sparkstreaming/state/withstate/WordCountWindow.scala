package com.vlion.sparkstreaming.state.withstate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountWindow {
    def main(args: Array[String]): Unit = {
       //TODO 3秒一个批次，窗口12秒，滑步6秒。

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
        val ssc = new StreamingContext(conf,Seconds(3))
        ssc.checkpoint(".ck")

        val lines:DStream[String] = ssc.socketTextStream("www.bigdata05.com",9999)
        val words: DStream[String] = lines.flatMap(_.split(""))

        val pairs = words.map((_,1))

        val wordCounts: DStream[(String, Int)] = pairs.reduceByKeyAndWindow((a:Int, b:Int) => (a + b), Seconds(12), Seconds(6))

        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()

    }
}
