package com.advlion.www.realtime.util

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * 接下来开始实时需求的分析，需要用到SparkStreaming来做实时数据的处理，
 * 在生产环境中，绝大部分时候都是对接的Kafka数据源，创建一个SparkStreaming读取Kafka数据的工具类。
 */
object MyKafkaUtil {

    //1.创建配置信息对象
    private val properties:Properties = PropertiesUtil.load("config.properties")

    //2.用于初始化链接到集群的地址
    val broker_list :String = properties.getProperty("kafka.broker.list")

    //3.kafka消费配置
    val kafkaParam = Map(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG-> broker_list,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        //消费者组
        "group.id"-> "test",
        //如果没有初始化偏移量或者当前偏移量不存在任何服务器上,可以使用这个属性配置
        //可以使用这个配置,latest自动重置偏移量为最新的偏移量,要加上,不是到当前时刻的偏移量,是这个消费者组最新的偏移量
        "auto.offset.reset" -> "latest",
        //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
        //如果是false，会需要手动维护kafka偏移量
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    //创建DStream,返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题
    def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
        dStream
    }

}
