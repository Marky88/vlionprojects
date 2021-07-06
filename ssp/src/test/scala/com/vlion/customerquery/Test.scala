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

        def randStr(n: Int) = (1 to n).map(x => alpha(Random.nextInt.abs % size)).mkString

        val rdd = sc.textFile("/tmp/test/20210331_imei_idfa_distinct_detail_day/2021-03-31")


        rdd.filter(line => line.startsWith("imei") && (line.length == 20 || line.length == 37)).mapPartitions(iter => {
            iter.map(s => {
                val arr = s.split("\001")
                if (arr(1).length == 32) {
                    randStr(10) + "|" + arr(1).toUpperCase()
                } else {
                    randStr(10) + "|" + getMd5(arr(1)).toUpperCase()
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

        val hdfsBasePath = "/data/spark/ssp/imei_idfa_oaid_day"
        val arr = List("2021-05-16",
            "2021-05-17",
            "2021-05-18",
            "2021-05-19",
            "2021-05-20",
            "2021-05-21",
            "2021-05-22",
            "2021-05-23",
            "2021-05-24",
            "2021-05-25",
            "2021-05-26",
            "2021-05-27",
            "2021-05-28",
            "2021-05-29",
            "2021-05-30",
            "2021-05-31",
            "2021-06-01",
            "2021-06-02",
            "2021-06-03",
            "2021-06-04",
            "2021-06-05",
            "2021-06-06",
            "2021-06-07",
            "2021-06-08",
            "2021-06-09",
            "2021-06-10",
            "2021-06-11",
            "2021-06-12",
            "2021-06-13",
            "2021-06-14",
            "2021-06-15",
            "2021-06-16",
            "2021-06-17",
            "2021-06-18",
            "2021-06-19",
            "2021-06-20",
            "2021-06-21",
            "2021-06-22",
            "2021-06-23",
            "2021-06-24",
            "2021-06-25",
            "2021-06-26",
            "2021-06-27",
            "2021-06-28",
            "2021-06-29",
            "2021-06-30",
            "2021-07-01",
            "2021-07-02",
            "2021-07-03",
            "2021-07-04")

        import spark.implicits._
        //def aggregate[B](z: =>B)(seqop: (B, A) => B, combop: (B, B) => B): B = foldLeft(z)(seqop)
        val tupleToLong = arr.map(date => {
            val rdd = sc.textFile(s"${hdfsBasePath}/${date}").mapPartitions(iter => {

                iter.map(line => {
                    val arr = line.split("\001")
                    ((date, arr(0)), arr(1)) // ((日期, 类型), id)
                })
            })
            rdd
        })
            .reduceLeft(_ union _)
            .countByKey()

        tupleToLong.toList.sortWith( (t1,t2) => {
            if(t1._1._1 == t2._1._1){
                t1._1._2 < t2._1._2
            }else{
                t1._1._1 < t2._1._1
            }
        }).foreach(println)



    }


    def getMd5(inputStr: String): String = {
        val md: MessageDigest = MessageDigest.getInstance("MD5")
        md.digest(inputStr.getBytes()).map(0xFF & _).map {
            "%02x".format(_)
        }.foldLeft("") {
            _ + _
        }
    }

}
