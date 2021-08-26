package com.advlion.www.realtime

import com.advlion.www.realtime.handler.AlloffbLogParseHandler
import com.advlion.www.realtime.model.Alloffb_log
import com.advlion.www.realtime.parse.ParseAlloffbLog
import com.advlion.www.realtime.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {
    def main(args: Array[String]): Unit = {
//集群
        val spark = SparkSession.builder()
            //      .master("local[*]")
            .appName("Zebra")
            .config("hive.metastore.uris", "thrift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
            .config("spark.debug.maxToStringFields", "100")
            .enableHiveSupport()
            .getOrCreate()
        val sc = spark.sparkContext
            sc.setLogLevel("WARN")
        val ssc = new StreamingContext(sc,Seconds(5))


////本地
//        //1.创建SparkConf
//        val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeApp").setMaster("local[*]")
//        //2.创建StreamingContext
//        val ssc = new StreamingContext(sparkConf,Seconds(3))
        ssc.checkpoint("./zebra_ck")


        //3.读取数据

        val kafkaDStream:InputDStream[ConsumerRecord[String,String]]= MyKafkaUtil.getKafkaStream(PropertiesUtil.load("config.properties").getProperty("kafka.topic"),ssc)

        //4.将从Kafka度去除的数据转换为样例类
        val alloffbLogDStream:DStream[Alloffb_log] = kafkaDStream
            .filter(_.value().contains("/api/v1/adsync"))
            .map(record => {
            val value = record.value()
//                println("=="+value)
            ParseAlloffbLog.parse(value)
        }).filter(_ != null)

//        alloffbLogDStream.print()
        //5.业务处理
        AlloffbLogParseHandler.addBatchReport(alloffbLogDStream)//生成目标RDD



        //启动任务
        ssc.start()
        ssc.awaitTermination()

    }
}
