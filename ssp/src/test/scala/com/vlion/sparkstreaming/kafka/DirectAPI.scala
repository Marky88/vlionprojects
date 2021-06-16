package com.vlion.sparkstreaming.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object DirectAPI {
    def main(args: Array[String]): Unit = {

        //定义更新状态的方法,参数values为当前批次单词频度,state 为以往批次单词频度
        val updateFunc  = (values:Seq[Int],state:Option[Int]) =>{
            val currentCount = values.foldLeft(0)(_+_)
            val previousCount = state.getOrElse(0)
            Some(currentCount+previousCount)
        }


        //创建SparkConf
        val sparkConf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiverWordCount")

        //2.创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("./ck")
        //3.定义Kafka参数
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "www.bigdata04.com:9092,www.bigdata05.com:9092,www.bigdata06.com:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )

        //4.读取Kafka数据创建DStream
        val kafkaDStream :InputDStream[ConsumerRecord[String, String]]= KafkaUtils.createDirectStream[String,String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String,String](Set("first"), kafkaPara)
        )


        //5.将每条消息的KV取出
        val valueDStream:DStream[String] = kafkaDStream.map(record => record.value())

        //6.计算WordCount
        val rsDStream = valueDStream.flatMap(_.split(","))
            .map((_, 1))
            .reduceByKey(_ + _)

//        val maxId = valueDStream.map(_.split(",")(1).toInt).reduce(Math.max)



         rsDStream.updateStateByKey(updateFunc).transform(rdd => {
             rdd.sortBy(_._2, false)
         })
            .print()

        //7.开启任务
        ssc.start()
        ssc.awaitTermination()




    }


}
