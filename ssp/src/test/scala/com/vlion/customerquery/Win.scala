package com.vlion.customerquery

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/1/29/0029 18:16
 *
 */
object Win {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            //     .master("local[*]")
            .appName("Ssp")
            .config("hive.metastore.uris", "trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
            .config("spark.debug.maxToStringFields", "100")
            .enableHiveSupport()
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        val sc = spark.sparkContext

        val rdd = sc.textFile("/tmp/test/storm_new_ssp_win_vlionserver_all.log")
         rdd.map(line => {
            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00")
            val arr = line.split("\t")
            val time = LocalDateTime.ofEpochSecond(arr(1).toLong, 0, ZoneOffset.ofHours(8))
            ((formatter.format(time), arr(2), arr(3)), if(arr(5) ==null || arr(5) =="") 0.0 else arr(5).toDouble) //dsp_id,adsolt_id,price
        })
            .reduceByKey(_+_)
            .map{ case ((time,dspId,adsoltId),price) =>
                s"${time}\t${dspId}\t$adsoltId\t${BigDecimal(price)}"
            }.saveAsTextFile("/tmp/test/out2012")





    }
}
