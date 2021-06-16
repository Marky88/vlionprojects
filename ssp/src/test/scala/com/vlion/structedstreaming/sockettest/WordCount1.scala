package com.vlion.structedstreaming.sockettest

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @author malichun
 * @date 2020/7/20 002018:40
 */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("WordCount1")
            .getOrCreate()
        import spark.implicits._
        val lines: DataFrame = spark.readStream
            .format("socket")
            .option("host", "www.bigdata05.com")
            .option("port", 9999)
            .load

        val result:StreamingQuery = lines.writeStream
            .format("console")
            .outputMode("update") //complete,append ,update更新模式
            .start

        result.awaitTermination()

        spark.stop()
    }
}
