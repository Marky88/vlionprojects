package com.vlion.sparkstreaming.state.nostate

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 无状态转化
 */
object NoState {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("noState").setMaster("local[*]")

        val ssc = new StreamingContext(conf,Seconds(3))
        //val ssc = new StreamingContext(sc,Seconds(3))
        //创建DStream
        val lineDStream:ReceiverInputDStream[String]=ssc.socketTextStream("www.bigdata05.com",9999)

        //转为RDD操作
        val wordAndCountDStream:DStream[(String,Int)] = lineDStream.transform(rdd => {
            val words:RDD[String] = rdd.flatMap(_.split(" "))
            val wordAndOne:RDD[(String,Int)] = words.map((_,1))
            val value: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
            value
        })

        //打印
        wordAndCountDStream.print(20)

        //启动
ssc.start()
        ssc.awaitTermination()
    }
}
