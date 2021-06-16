package com.vlion.spark

import net.ipip.ipdb.City
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession




/**
 * @description:
 * @author: malichun
 * @time: 2020/11/20/0020 15:14
 *    2 同一个媒体中的ip分布，是否反常，是否会集中在某些地域
 */
object CheapIPAppAnalysis {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("TEST_IP_CITY").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder().getOrCreate()

        import spark.implicits._


        val etlDate = "2020-11-19"
        val etlHour = "12"
        val reqDF = spark.sql(
            s"""
               |select
               |   distinct app_id,ip
               |from
               |ods.ods_media_req
               |where etl_date='${etlDate}' and etl_hour='${etlHour}'
               |and ip regexp '^\\\\d[\\\\d.]+\\\\d$$' --ip过滤
               |""".stripMargin
        )


        val tempRDD = reqDF.rdd.map(row => (row.getAs[String]("app_id"), row.getAs[String]("ip")))
            .repartition(30)
            .mapPartitions(iter => {
                val configuration = new Configuration()
                val fs = FileSystem.get(configuration)
                val path = new Path("hdfs://www.bigdata02.com:8020/tmp/test/0330.ipdb");
                val in = fs.open(path);

                val db = new City(in)
                val res: Iterator[(String, String, String, String, String)] = iter.flatMap(tuple => {
                    val app_id = tuple._1
                    val ip = tuple._2

                    val info = db.findInfo(ip, "CN")
                    //                    info
                    val country = info.getCountryName
                    val province = info.getRegionName
                    val city = info.getCityName
                    if (country != "" ) List((country,app_id, province, city, ip)) else List()
                })
                in.close()
                fs.close()
                res
            })

        tempRDD.toDF("country_name","app_id","province_name","city_name","ip").createOrReplaceTempView("tempView")

        spark.sql(
            s"""
               |select
               |country_name,app_id,province_name,city_name,count(1) as ip_cnt
               |from
               |    tempView
               |group by
               |    country_name,app_id,province_name,city_name
               |""".stripMargin)
            .write
            .format("jdbc")
            .option("url", "jdbc:mysql://172.16.197.73:3306/ssp_report?useUnicode=true&characterEncoding=utf8")
            .option("dbtable", "ip_app_id_analysis")
            .option("user", "root")
            .option("password", "123456")
            .mode("append")
            .save()


        sc.stop()



    }
}
