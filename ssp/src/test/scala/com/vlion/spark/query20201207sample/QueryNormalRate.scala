package com.vlion.spark.query20201207sample

import com.vlion.spark.util.GetIpFront
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

/**
 * @description:
 * @author: malichun
 * @time: 2020/12/11/0011 17:46
 *  检查正常的媒体被过滤的占比
 */
object QueryNormalRate {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .enableHiveSupport()
            .getOrCreate()
        val sc = new SparkContext()


        val etlDate = "2020-12-11"
        val etlHour = "16"

        val tableName = "ods.ods_media_req"

        spark.udf.register("get_ip_front",GetIpFront.getIPFront)

        val frame: DataFrame = spark.sql(
            s"""
               |select
               |app_id,
               |k,v
               |from
               |(
               |select
               |app_id,ip,imei,idfa,android_id,get_ip_front(ip) as ip3
               |from ods.ods_media_req
               |where etl_date='${etlDate}' and etl_hour='${etlHour}'
               |) t
               |lateral view explode(map('imei',imei,'idfa',idfa,'ip',ip,'anid',android_id,'ip3',ip3)) m as k,v
               |where (v is not null and v !='' and v!='\\\\N' and (not v regexp '^[0-]*$$') and v !='没有权限' and v != 'unknown')
               |        or ( k = 'ip' and ip regexp '((2(5[0-5]|[0-4]\\\\d))|[0-1]?\\\\d{1,2})(\\\\.((2(5[0-5]|[0-4]\\\\d))|[0-1]?\\\\d{1,2})){3}' )
               |
               |""".stripMargin)
        frame.rdd
            .repartition(30)
            .mapPartitions(iter => {
                val rc = new Jedis("172.16.189.215", 6379)
                rc.select(0)
                iter.map(
                    r => {
                        val appId = r.getAs[String]("app_id")
                        val k = r.getAs[String]("k")
                        var id = r.getAs[String]("v")
                        val redisKey = "ct:" + k + ":" + id
                        val flag = rc.exists(redisKey)
                        if (flag) ((appId,k,"exists"), 1) else ((appId,k,"not_exists"), 1)
                    })
            })
            .countByKey()
            .foreach(println)


    }


}
