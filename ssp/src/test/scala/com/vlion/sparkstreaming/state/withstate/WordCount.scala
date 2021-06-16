package com.vlion.sparkstreaming.state.withstate

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * updateStateByKey
 */
object WordCount {
    def main(args: Array[String]): Unit = {

        //定义更新状态的方法,参数values为当前批次单词频度,state 为以往批次单词频度
        val updateFunc  = (values:Seq[Int],state:Option[Int]) =>{
            val currentCount = values.foldLeft(0)(_+_)
            val previousCount = state.getOrElse(0)
            Some(currentCount+previousCount)
        }

        val conf = new SparkConf().setMaster("local[*]").setAppName("wordCountUpdateStateByKey")
//        val sc = new SparkContext(conf)

//        sc.setLogLevel("WARN")

        val ssc = new StreamingContext(conf,Seconds(5))
        ssc.checkpoint("./ck")

        //创建DStream
        val lines = ssc.socketTextStream("www.bigdata05.com",9999)

        //
        val words = lines.flatMap(_.split(" "))

        val pairs: DStream[(String, Int)] = words.map(word => (word, 1))

        //使用updateStateByKey来更新状态,统计从运行开始以来单词总的次数
        val stateDstream: DStream[(String, Int)] = pairs.updateStateByKey(updateFunc)

        stateDstream.print()

        ssc.start()
        ssc.awaitTermination()



    }
}
