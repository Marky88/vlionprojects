package com.vlion.customerquery

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/4/8/0008 10:25
 *
 *        需要看一下猛犸流量里面的imei和idfa的数量，原文和MD5都要，
 *        你这几天每天统计一次，把数据拉出来，去重。看一下每天多少条，3天去重，7天去重多少条。
 */
object ImeiIdfaCount20210408 {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("123"))
        val spark = SparkSession.builder().getOrCreate()

        import spark.implicits._

        val dates = Array("2021-03-31", "2021-04-01", "2021-04-02", "2021-04-03"
            , "2021-04-04", "2021-04-05", "2021-04-06")
        val files = dates.map(date => (date, "/tmp/test/20210331_imei_idfa_distinct_detail_day/" + date))

        files.map { case (date, filePath) =>
            convertRDD(date, sc.textFile(filePath))
        }.map(rdd => {
            val df = rdd.toDF("date", "type", "id")
            df
        }).reduce(_ union _)
            .createOrReplaceTempView("unionDF")


        spark.sql(
            s"""
               |
               |select
               |date,type,count(1)
               |from
               |    unionDF
               |group by date,type
               |order by date,type
               |
               |""".stripMargin)
            .show(1000, false)


        //递增新增多少
        spark.sql(
            s"""
               |select
               |    type,rn,cnt,count(1)
               |from
               |(
               |select
               |    type,id,rn,count(1) as cnt
               |from
               |(
               |    select
               |        date,type,id,
               |        dense_rank() over(partition by type order by date) as rn
               |    from
               |        unionDF
               |
               |    union all
               |
               |    select
               |        date,type,id,
               |        dense_rank() over(partition by type order by date) + 1 as rn
               |    from
               |        unionDF
               |) t
               |group by type,id,rn
               |) t
               |group by type,rn,cnt
               |
               |
               |""".stripMargin)
            .show(1000, truncate = false)


        spark.sql(
            s"""
               |select
               |    flag,type,cnt1,count(1)
               |from
               |(
               |select
               |    flag,type,id,count(1) as cnt1
               |from
               |(
               |select
               |    date,type,id,flag
               |from
               |(
               |      select
               |          t1.date,t1.type,t1.id,t2.flag1,t2.flag2
               |      from
               |          unionDF t1
               |      inner join
               |      (
               |          select
               |             '2021-03-31' as dt,'1' as flag1, '0' as flag2
               |          union all
               |          select
               |             '2021-04-01' as dt,'2' as flag1, '1' as flag2
               |          union all
               |          select
               |             '2021-04-02' as dt,'3' as flag1, '2' as flag2
               |          union all
               |          select
               |             '2021-04-03' as dt,'4' as flag1, '3' as flag2
               |          union all
               |          select
               |             '2021-04-04' as dt,'5' as flag1, '4' as flag2
               |          union all
               |          select
               |             '2021-04-05' as dt,'6' as flag1, '5' as flag2
               |          union all
               |          select
               |             '2021-04-06' as dt,'7' as flag1, '6' as flag2
               |      ) t2
               |      on t1.date = t2.dt
               |) t
               |lateral view explode( array(flag1,flag2)) m as flag
               |) t
               |group by flag,type,id
               |) t
               |group by flag,type,cnt1
               |
               |""".stripMargin)







        // 7天去重多少条。
        spark.sql(
            s"""
               |select
               |    type,count(distinct id) as cnt
               |from
               |unionDF
               |group by type
               |""".stripMargin).show(1000, false)

        // 3天去重多少条
        spark.sql(
            s"""
               |
               |    select
               |        date,type,id,
               |        dense_rank() over(partition by type order by date) as rn
               |    from
               |        unionDF
               |
               |    union all
               |
               |    select
               |        date,type,id,
               |        dense_rank() over(partition by type order by date) + 1 as rn
               |    from
               |    unionDF
               |
               |    union all
               |
               |    select
               |        date,type,id,
               |        dense_rank() over(partition by type order by date) + 1 as rn
               |    from
               |        unionDF
               |
               |
               |""".stripMargin)

        spark.sql(
            s"""
               |select type,count(distinct id) from unionDF where date in ('2021-03-31','2021-04-01', '2021-04-02') group by type
               |
               |""".stripMargin)



    }


    def convertRDD(date: String, rdd: RDD[String]) = {
        rdd.mapPartitions(iter => {
            iter.map(line => line.split("\\001"))
                .collect {
                    case arr if arr.length == 2 => (date,arr(0),arr(1))
                }
        })
    }


}
