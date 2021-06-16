package com.vlion.sparkstreaming.state.nostate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @author malichun
 * @date 2020/7/21 002113:10
 */
object JoinTest {
    def main(args: Array[String]): Unit = {

        //1.创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JoinTest")

        //2.创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        //3.从端口获取数据创建流
        val lineDStream1: ReceiverInputDStream[String] = ssc.socketTextStream("www.bigdata05.com", 9999)
        val lineDStream2: ReceiverInputDStream[String] = ssc.socketTextStream("www.bigdata01.com", 10000)

        //4.将两个流转换为KV类型
        val wordToOneDStream: DStream[(String, Int)] = lineDStream1.flatMap(_.split(" ")).map((_, 1))
        val wordToADStream: DStream[(String, String)] = lineDStream2.flatMap(_.split(" ")).map((_, "a"))

        //5.流的JOIN
        val joinDStream: DStream[(String, (Int, String))] = wordToOneDStream.join(wordToADStream)

        //6.打印
        joinDStream.print()

        //7.启动任务
        ssc.start()
        ssc.awaitTermination()
    }
}
