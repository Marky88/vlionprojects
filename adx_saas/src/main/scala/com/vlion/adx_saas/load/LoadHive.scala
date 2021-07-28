package com.vlion.adx_saas.load

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * @description:
 * @author: malichun
 * @time: 2021/7/8/0008 18:02
 *
 */
object LoadHive {

    def loadHive(implicit spark: SparkSession, etlDate: String, etlHour: String): Unit = {
        val meidiaReqDF = spark.sql( // 同一个请求id会有一个
            s"""
               |select
               |    t1.request_id,
               |    max(t1.app_id) as media_id,
               |    max(t1.adsolt_id) as posid_id, -- 广告位id
               |    max(t2.id) as pkg_id,
               |    max(t3.id) as country_id,
               |    max(t4.id) as platform_id,
               |    max(t5.id) as style_id,
               |    max(t6.mlevel_id) as mlevel_id,
               |    1 as ssp_req,  -- 请求id聚合count
               |    sum(t1.req_floor_price) as sum_ssp_floor_price -- 只有1个,有价格的算,没价格的不算,后面求sum
               |from
               |    ods.adx_saas_media_req t1
               |left join
               |    pkg t2
               |on t1.pkg_name = t2.name
               |left join country t3
               |on t1.country = t3.jsonkey
               |left join platform t4
               |on t1.os = t4.jsonkey
               |left join style t5
               |on t1.adsolt_type = t5.jsonkey
               |left join media t6
               |on t1.app_id = t6.id
               |where etl_date='${etlDate}' and etl_hour = '$etlHour'
               |group by
               |t1.request_id
               |
               |""".stripMargin)
            .persist(StorageLevel.MEMORY_AND_DISK)
            .createOrReplaceTempView("meidiaReq")


        val dspReqDF = spark.sql( // 同一个请求id会有多个
            s"""
               |select
               |    request_id,
               |    dsp_id,
               |    1 as dsp_req, -- 多个
               |    count(if(staus_code='1',1,null)) as dsp_fill_req,
               |    sum(if(dsp_bid_price is null or trim(dsp_bid_price)='',null,dsp_bid_price)) as sum_dsp_floor_price, -- 有多个
               |    count(if(staus_code='201',1,null)) as dsp_req_timeout, -- 不去重
               |    count(if(staus_code='202',1,null)) as dsp_req_parse_error, -- 不去重
               |    count(if(staus_code='203',1,null)) as dsp_req_invalid_ad,
               |    count(if(staus_code='206',1,null)) as dsp_req_no_bid
               |from
               |    ods.adx_saas_dsp_req
               |where etl_date='${etlDate}' and etl_hour = '$etlHour'
               |group by request_id,dsp_id
               |
               |""".stripMargin).createOrReplaceTempView("dspReq")

        // 后面同一个请求id会有一个
        val mediaRespDF = spark.sql(
            s"""
               |select
               |    request_id,
               |    dsp_id,
               |    1 as dsp_win,
               |    max(dsp_bid) as dsp_win_price
               |from
               |    ods.adx_saas_media_resp
               |where etl_date='${etlDate}' and etl_hour = '$etlHour'
               |group by request_id,dsp_id
               |""".stripMargin)
            .persist(StorageLevel.MEMORY_AND_DISK)
            .createOrReplaceTempView("mediaResp")


        val joinedDF = spark.sql(
            s"""
               |select
               |    t1.request_id,
               |    t2.dsp_id,
               |    t1.media_id,
               |    t1.posid_id,
               |    t1.pkg_id,
               |    t1.country_id,
               |    t1.platform_id,
               |    t1.style_id,
               |    t1.mlevel_id,
               |    null as ssp_req, -- 后面需要去重*****
               |    t2.dsp_req,
               |    t2.dsp_fill_req,
               |    t3.dsp_win as dsp_win, -- 媒体返回
               |    null as ssp_win,
               |    t2.sum_dsp_floor_price,
               |    null as sum_ssp_floor_price, -- 只有1个,有价格的算,没价格的不算,后面求avg
               |    t3.dsp_win_price,      -- 媒体返回
               |    null as imp,
               |    null as clk,
               |    null as revenue,
               |    null as cost,
               |    t2.dsp_req_timeout,
               |    t2.dsp_req_parse_error,
               |    t2.dsp_req_invalid_ad,
               |    t2.dsp_req_no_bid
               |from
               |    meidiaReq t1
               |left join
               |    dspReq t2
               |on t1.request_id = t2.request_id
               |left join
               |    mediaResp t3
               |on t2.request_id = t3.request_id and t2.dsp_id = t3.dsp_id
               |""".stripMargin)

        val impDF = spark.sql(
            s"""
               |select
               |    request_id,
               |    dsp_id, -- 定向id,后面需要转换
               |    null as media_id,
               |    null as posid_id,
               |    null as pkg_id,
               |    null as country_id,
               |    null as platform_id,
               |    null as style_id,
               |    null as mlevel_id,
               |    null as ssp_req,
               |    null as dsp_req,
               |    null as dsp_fill_req,
               |    null as dsp_win,
               |    null as ssp_win,
               |    null as sum_dsp_floor_price,
               |    null as sum_ssp_floor_price,
               |    null as dsp_win_price,
               |    1 as imp, -- 每个曝光1条
               |    null as clk,
               |    null as revenue,
               |    null as cost,
               |    null as dsp_req_timeout,
               |    null as dsp_req_parse_error,
               |    null as dsp_req_invalid_ad,
               |    null as dsp_req_no_bid
               |from
               |ods.adx_saas_imp
               |where etl_date='${etlDate}' and etl_hour = '$etlHour'
               |group by
               |    request_id,dsp_id
               |""".stripMargin)


        val clkDF = spark.sql(
            s"""
               |select
               |    request_id,
               |    dsp_id,
               |    null as media_id,
               |    null as posid_id,
               |    null as pkg_id,
               |    null as country_id,
               |    null as platform_id,
               |    null as style_id,
               |    null as mlevel_id,
               |    null as ssp_req,
               |    null as dsp_req,
               |    null as dsp_fill_req,
               |    null as dsp_win,
               |    null as ssp_win,
               |    null as sum_dsp_floor_price,
               |    null as sum_ssp_floor_price,
               |    null as dsp_win_price,
               |    null as imp,
               |    1 as clk, -- 每个曝光1条
               |    null as revenue,
               |    null as cost,
               |    null as dsp_req_timeout,
               |    null as dsp_req_parse_error,
               |    null as dsp_req_invalid_ad,
               |    null as dsp_req_no_bid
               |from
               |    ods.adx_saas_clk
               |    where etl_date='${etlDate}' and etl_hour = '$etlHour'
               |group by
               |    request_id,dsp_id
               |""".stripMargin)

        val dspWinDF = spark.sql(
            s"""
               |select
               |    request_id,
               |    dsp_id,
               |    null as media_id,
               |    null as posid_id,
               |    null as pkg_id,
               |    null as country_id,
               |    null as platform_id,
               |    null as style_id,
               |    null as mlevel_id,
               |    null as ssp_req,
               |    null as dsp_req,
               |    null as dsp_fill_req,
               |    null as dsp_win,
               |    1 as ssp_win,
               |    null as sum_dsp_floor_price,
               |    null as sum_ssp_floor_price,
               |    null as dsp_win_price,
               |    null as imp,
               |    null as clk,
               |    max(dsp_final_price) as revenue, -- 只有1个,成交价----------------
               |    max(ssp_final_price) as cost, -- 只有1个
               |    null as dsp_req_timeout,
               |    null as dsp_req_parse_error,
               |    null as dsp_req_invalid_ad,
               |    null as dsp_req_no_bid
               |from
               |    ods.adx_saas_dsp_win
               |where etl_date='${etlDate}' and etl_hour = '$etlHour'
               |group by request_id,dsp_id
               |""".stripMargin)

        val unionDF = joinedDF union impDF union clkDF union dspWinDF

        unionDF.createOrReplaceTempView("u1")
        spark.sql(
            """
              |select
              | request_id,
              | dsp_id,
              | max(media_id) as media_id,
              | max(posid_id) as posid_id,
              | max(pkg_id) as pkg_id,
              | max(country_id) as country_id,
              | max(platform_id) as platform_id,
              | max(style_id) as style_id,
              | max(mlevel_id) as mlevel_id,
              | null as ssp_req, -- ssp_req,有问题..,-1的数据
              | max(dsp_req) as dsp_req,
              | sum(dsp_fill_req) as dsp_fill_req,
              | max(dsp_win) as dsp_win,
              | max(ssp_win) as ssp_win,
              | sum(sum_dsp_floor_price) as sum_dsp_floor_price,
              | null as sum_ssp_floor_price,   -- -1的数据
              | sum(dsp_win_price) as dsp_win_price,
              | max(imp) imp,
              | max(clk) clk,
              | max(revenue) revenue,
              | max(cost) cost,
              | sum(dsp_req_timeout) dsp_req_timeout,
              | sum(dsp_req_parse_error) dsp_req_parse_error,
              | sum(dsp_req_invalid_ad) dsp_req_invalid_ad,
              | sum(dsp_req_no_bid) as dsp_req_no_bid
              |from
              | u1
              |group by
              | request_id,dsp_id -- 每个请求id+ dsp_id唯一
              |""".stripMargin).createOrReplaceTempView("union_table")


    }

}
