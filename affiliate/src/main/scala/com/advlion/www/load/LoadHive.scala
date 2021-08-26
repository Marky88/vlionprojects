package com.advlion.www.load

import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2020/11/5/0005 12:48
 *
 */
object LoadHive {

    //根据需求跑当前的数据
    def readHive(spark:SparkSession, etlDate:String,etlHour:String):Unit ={

        val affiliateClkDF = spark.sql(
            s"""
               |select
               |    logtype,
               |	time,
               |	media_id,
               |	offer_id,
               |	source_id,
               |	sub_channel,
               |	count(1) click_count,
               |	0 activate_count,
               |	0 shave_activates
               |from
               |    ods.ods_affiliate_clk
               |where etl_date='${etlDate}'
               |group by logtype,time,media_id,offer_id,source_id,sub_channel
               |""".stripMargin)

        val affiliateCallBackDF = spark.sql(
            s"""
               |select	logtype,
               |		time,
               |		media_id,
               |		offer_id,
               |		source_id,
               |		sub_channel,
               |		0 click_count,
               |		count(1) activate_count,
               |		count(case when referrer='true' then 1 end)	 shave_activates
               |from	ods.ods_affiliate_callback
               |where	etl_date='$etlDate'
               |group	by logtype,time,media_id,offer_id,source_id,sub_channel
               |
               |""".stripMargin)

        affiliateClkDF.union(affiliateCallBackDF).createOrReplaceTempView("unionDF")

    }
}
