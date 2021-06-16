package com.vlion.spark.query20201130

import java.lang

import com.vlion.spark.summer.TEngine
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
 * @description:
 * @author: malichun
 * @time: 2020/11/30/0030 15:38
 *        帮我统计一下，昨天类型202日志。
 *        按照广告位id分，第7个字段（出价），统计平均值，和数值分段比例
 *
 *         条件 第5个字段=1
 *
 */
object Query202log extends App with TEngine{
    start("spark"){
        val sc: SparkContext = env.asInstanceOf[SparkContext]
        val spark= SparkSession.builder().getOrCreate()

        import spark.implicits._

        spark.udf.register("get_ip_front",(ip:String) => {
            if(ip ==null ){
                null
            }else{
                val arr=ip.split("\\.")
                if(arr.length != 4){
                    null
                }else{
                    arr(0).toInt.toHexString+arr(1).toInt.toHexString+arr(2).toInt.toHexString
                }
            }
        })


       val df =  spark.sql(
            """
              |select
              |distinct
              |app_id,k,v
              |from
              |(
              |select
              | app_id,imei,idfa,ip,android_id
              |from
              |ods.ods_media_req
              |where etl_date='2020-12-02' and etl_hour='08'
              |and app_id in ('21012','30135','30136','30121','30120','30228','21015','20856','20426','20376')
              |) t
              |lateral view explode(map('imei',imei,'idfa',idfa,'ip',ip,'anid',android_id,'ip3',get_ip_front(ip))) m as k,v
              |where v is not null and v !='' and v!='\\N' and (not v regexp '^[0-]*$')  and v !='没有权限' and v != 'unknown'
              |""".stripMargin)
//--
          df.rdd.repartition(30)
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





        Array(
            ((20376,"idfa","not_exists"),6275)         ,
            ((30120,"imei","not_exists"),241389)       ,
            ((20856,"ip","exists"),1667)               ,
            ((20376,"ip","exists"),917)                ,
            ((30228,"ip","not_exists"),9378)           ,
            ((21012,"ip","not_exists"),945)            ,
            ((21015,"idfa","not_exists"),9636)         ,
            ((21012,"ip","exists"),22678)              ,
            ((30120,"ip","exists"),22606)              ,
            ((30135,"ip3","not_exists"),1502)          ,
            ((20376,"ip3","not_exists"),2780)          ,
            ((21015,"ip3","exists"),565)               ,
            ((21012,"imei","exists"),3)                ,
            ((20856,"ip3","not_exists"),4762)          ,
            ((20376,"ip3","exists"),498)               ,
            ((20426,"ip","exists"),1021)               ,
            ((30136,"anid","not_exists"),299112)       ,
            ((30136,"ip","not_exists"),1663)           ,
            ((30228,"imei","not_exists"),10081)        ,
            ((20856,"ip","not_exists"),5816)           ,
            ((20376,"ip","not_exists"),3291)           ,
            ((21012,"ip3","exists"),1898)              ,
            ((30120,"ip","not_exists"),875)            ,
            ((30228,"ip3","not_exists"),7828)          ,
            ((21012,"imei","not_exists"),348043)       ,
            ((21015,"ip3","not_exists"),3542)          ,
            ((30121,"idfa","not_exists"),133921)       ,
            ((30135,"ip","exists"),22720)              ,
            ((30120,"anid","exists"),2)                ,
            ((20856,"idfa","not_exists"),20430)        ,
            ((30136,"ip3","exists"),2128)              ,
            ((30136,"ip","exists"),22883)              ,
            ((30135,"ip3","exists"),1955)              ,
            ((30120,"imei","exists"),2)                ,
            ((21012,"anid","not_exists"),347798)       ,
            ((20426,"ip3","exists"),525)               ,
            ((30136,"ip3","not_exists"),1992)          ,
            ((30136,"anid","exists"),4)                ,
            ((30120,"ip3","not_exists"),1392)          ,
            ((21015,"ip","not_exists"),4251)           ,
            ((20426,"idfa","not_exists"),7361)         ,
            ((30135,"ip","not_exists"),1032)           ,
            ((30121,"ip","not_exists"),869)            ,
            ((30120,"ip3","exists"),1886)              ,
            ((30135,"idfa","not_exists"),217141)       ,
            ((30228,"anid","exists"),2)                ,
            ((21012,"ip3","not_exists"),1450)          ,
            ((30228,"ip","exists"),691)                ,
            ((21015,"ip","exists"),1110)               ,
            ((30121,"ip3","not_exists"),1387)          ,
            ((30228,"imei","exists"),2)                ,
            ((30121,"ip3","exists"),1887)              ,
            ((30136,"imei","exists"),5)                ,
            ((30228,"ip3","exists"),603)               ,
            ((20426,"ip3","not_exists"),3101)          ,
            ((30228,"anid","not_exists"),10089)        ,
            ((30120,"anid","not_exists"),241251)       ,
            ((21012,"anid","exists"),3)                ,
            ((30121,"ip","exists"),22561)              ,
            ((30136,"imei","not_exists"),299069)       ,
            ((20426,"ip","not_exists"),3690)           ,
            ((20856,"ip3","exists"),690)
        ).map(t => (t._1._1,t._1._2,t._1._3,t._2))
            .groupBy( t => (t._1,t._2))
            .map( t => {
                val key: (Int, String) = t._1
                val value: Array[(Int, String, String, Int)] = t._2
//                value.foldLeft(mutable.Map[String,Int]())((m, v) =>{
//                    m(v._3) = v._4
//                    m
//                }
                val m = ( mutable.Map[String,Int]() /: value )((m, v) =>{
                                        m(v._3) = v._4
                                        m
                                    })

                val existsValue= m.getOrElse("exists",0)
                val notExistsValue = m.getOrElse("not_exists",0)

                key._1+"\t"+key._2+"\t"+existsValue+"\t"+notExistsValue+"\t"+(existsValue+notExistsValue)
            })
            .foreach(println(_))


















    }


}
