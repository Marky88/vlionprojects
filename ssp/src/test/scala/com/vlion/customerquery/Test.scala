package com.vlion.customerquery

import java.security.MessageDigest

import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/31/0031 17:01
 *
 */
object Test {
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
        import spark.implicits._

        val alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        val size = alpha.size

        def randStr(n:Int) = (1 to n).map(x => alpha(Random.nextInt.abs % size)).mkString

        val rdd = sc.textFile("/tmp/test/20210331_imei_idfa_distinct_detail_day/2021-03-31")



        rdd.filter(line => line.startsWith("imei") && (line.length == 20 || line.length == 37)).mapPartitions(iter => {
            iter.map(s => {
                val arr = s.split("\001")
                if(arr(1).length == 32){
                    randStr(10)+"|"+arr(1).toUpperCase()
                }else{
                    randStr(10)+"|"+getMd5(arr(1)).toUpperCase()
                }

            })
        }).distinct.saveAsTextFile("/tmp/test/20210331_imei_idfa_distinct_detail_day/2021-03-31_out")



//        value.toDF("imei").createOrReplaceTempView("tab")


//        spark.sql(
//            s"""
//               |insert overwrite directory '/tmp/test/20210331_imei_idfa_distinct_detail_day/out_2021-03-31'
//               |select
//               |    concat(substr(regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", ""),1,10) ,'|',
//               |        if(length(imei) =32 , upper(imei),upper(md5(imei)))
//               |    )
//               |from
//               |tab
//               |""".stripMargin)



    }


    def getMd5(inputStr: String): String = {
        val md: MessageDigest = MessageDigest.getInstance("MD5")
        md.digest(inputStr.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") {_ + _}
    }

}
