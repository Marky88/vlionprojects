package com.advlion.www.task.customer_task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/4/0004 9:35
 *      oss的imei文件和库里面的imei 交集
 */
object SspImeiIntersection {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession
            .builder()
            //     .master("local[*]")
            .appName("SspImeiIntersection")
            .config("hive.metastore.uris","trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir","/user/hive/warehouse")
            .config("spark.debug.maxToStringFields","100")
            .enableHiveSupport()
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        //oss当天和我们昨天的数据做关联
        val etlDate = args(0)  //ods.ods_media_req的时间,(正常情况下昨天)

        val reqImei: RDD[String] = spark.sql(
            s"""select
               |    distinct if(length(imei) != 32,md5(imei),imei) as imei
               |from ods.ods_media_req
               |where
               |etl_date='$etlDate'
               |and imei !='000000000000000'
               |and imei is not null and imei != ''
               |
               |""".stripMargin)
            .rdd
            .map(row => row.getAs[String](0))
//        reqImei.saveAsTextFile("/data/ssp_imei_intersection/output1")

        val ossImei = spark.sparkContext.textFile("hdfs://www.bigdata02.com:8020/data/ssp_imei_intersection/input").distinct

        val resRDD = ossImei.intersection(reqImei).coalesce(10)
        resRDD.saveAsTextFile("hdfs://www.bigdata02.com:8020/data/ssp_imei_intersection/output")





    }

}
