package com.vlion.adx_saas.load

import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/7/8/0008 18:02
 *
 */
object LoadHive {

    def loadHive(implicit spark: SparkSession, etlDate: String, etlHour: String): Unit = {
        val meidiaReqDF = spark.sql(
            s"""
               |select
               |    t1.request_id,
               |    null as dsp_id,
               |    max(t1.app_id) as media_id,
               |    max(t1.adsolt_id) as posid_id,
               |    max(t2.id) as pkg_id,
               |    max(t3.id) as country_id,
               |    max(t4.id) as platform_id,
               |    max(t5.id) as style_id,
               |    max(t6.mlevel_id) as mlevel_id,
               |    null as dsp_req,
               |    null as dsp_fill_req,
               |    null as dsp_win,
               |    null as ssp_win,
               |    null as sum_dsp_floor_price,
               |    null as count_dsp_floor_price,
               |    sum(t1.req_floor_price) as sum_ssp_floor_price, -- 只有1个,有价格的算,没价格的不算,后面求avg
               |    null as imp,
               |    null as clk,
               |    null as revenue,
               |    null as cost,
               |    null as dsp_req_timeout,
               |    null as dsp_req_parse_error,
               |    null as dsp_req_invalid_ad,
               |    null as dsp_req_no_bid
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
               |""".stripMargin).createOrReplaceTempView("meidiaReq")


        val dspReqDF = spark.sql(
            s"""
               |select
               |    request_id,
               |    dsp_id as dsp_id, -- dsp定向id
               |    null as media_id,
               |    null as posid_id,
               |    null as pkg_id,
               |    null as country_id,
               |    null as platform_id,
               |    null as style_id,
               |    null as mlevel_id,
               |    1 as dsp_req, -- 多个
               |    count(if(staus_code='1',1,null)) as dsp_fill_req,
               |    null as dsp_win,
               |    null as ssp_win,
               |    sum(if(dsp_floor_price is null or trim(dsp_floor_price)='',null,dsp_floor_price)) as sum_dsp_floor_price, -- 有多个
               |    count(if(dsp_floor_price is null or trim(dsp_floor_price)='',null,dsp_floor_price)) as count_dsp_floor_price, -- 底价有的才统计
               |    null as sum_ssp_floor_price,
               |    null as imp,
               |    null as clk,
               |    null as revenue,
               |    null as cost,
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
               |    t2.dsp_req,
               |    t2.dsp_fill_req,
               |    null as dsp_win,
               |    null as ssp_win,
               |    t2.sum_dsp_floor_price,
               |    t2.count_dsp_floor_price,
               |    t1.sum_ssp_floor_price, -- 只有1个,有价格的算,没价格的不算,后面求avg
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
               |""".stripMargin)

//
//        val mediaRespDF = spark.sql(
//            s"""
//               |
//               |select
//               |    t1.request_id,
//               |    max(t2.dsp_id) as dsp_id,
//               |    null as media_id,
//               |    null as posid_id,
//               |    null as pkg_id,
//               |    null as country_id,
//               |    null as platform_id,
//               |    null as style_id,
//               |    null as mlevel_id,
//               |    null as dsp_req,
//               |    null as dsp_fill_req,
//               |    null as dsp_win,
//               |    null as ssp_win,
//               |    null as sum_dsp_floor_price,
//               |    null as count_dsp_floor_price,
//               |    null as sum_ssp_floor_price,
//               |    null as imp,
//               |    null as clk,
//               |    null as revenue,
//               |    null as cost,
//               |    null as dsp_req_timeout,
//               |    null as dsp_req_parse_error,
//               |    null as dsp_req_invalid_ad,
//               |    null as dsp_req_no_bid
//               |from
//               |    ods.adx_saas_media_resp t1
//               |left join target t2
//               |on t1.dsp_id = t2.id
//               |where etl_date='${etlDate}' and etl_hour = '$etlHour'
//               |group by t1.request_id
//               |
//               |""".stripMargin)

   /*     val mediaRespDF = spark.sql(
            s"""
               |
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
               |    null as dsp_req,
               |    null as dsp_fill_req,
               |    null as dsp_win,
               |    null as ssp_win,
               |    null as sum_dsp_floor_price,
               |    null as count_dsp_floor_price,
               |    null as sum_ssp_floor_price,
               |    null as imp,
               |    null as clk,
               |    null as revenue,
               |    null as cost,
               |    null as dsp_req_timeout,
               |    null as dsp_req_parse_error,
               |    null as dsp_req_invalid_ad,
               |    null as dsp_req_no_bid
               |from
               |    ods.adx_saas_media_resp
               |where etl_date='${etlDate}' and etl_hour = '$etlHour'
               |group by t1.request_id
               |
               |""".stripMargin)*/


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
               |    null as dsp_req,
               |    null as dsp_fill_req,
               |    null as dsp_win,
               |    null as ssp_win,
               |    null as sum_dsp_floor_price,
               |    null as count_dsp_floor_price,
               |    null as sum_ssp_floor_price,
               |    1 as imp, -- 每个曝光1条
               |    null as clk,
               |    null as revenue,
               |    null as cost,
               |    null as dsp_req_timeout,
               |    null as dsp_req_parse_error,
               |    null as dsp_req_invalid_ad,
               |    null as dsp_req_no_bid
               |from
               |    ods.adx_saas_imp
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
               |    null as dsp_req,
               |    null as dsp_fill_req,
               |    null as dsp_win,
               |    null as ssp_win,
               |    null as sum_dsp_floor_price,
               |    null as count_dsp_floor_price,
               |    null as sum_ssp_floor_price,
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
               |where etl_date='${etlDate}' and etl_hour = '$etlHour'
               |group by
               |    request_id,dsp_id
               |""".stripMargin)

        val sspWinDF = spark.sql(
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
               |    null as dsp_req,
               |    null as dsp_fill_req,
               |    null as dsp_win,
               |    1 as ssp_win, -- 外面再聚合时sum
               |    null as sum_dsp_floor_price,
               |    null as count_dsp_floor_price,
               |    null as sum_ssp_floor_price,
               |    null as imp,
               |    null as clk,
               |    null as revenue,
               |    null as cost,
               |    null as dsp_req_timeout,
               |    null as dsp_req_parse_error,
               |    null as dsp_req_invalid_ad,
               |    null as dsp_req_no_bid
               |from
               |    ods.adx_saas_ssp_win
               |where
               |    etl_date='${etlDate}' and etl_hour = '$etlHour'
               |group by request_id,dsp_id
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
               |    null as dsp_req,
               |    null as dsp_fill_req,
               |    1 as dsp_win,
               |    null as ssp_win,
               |    null as sum_dsp_floor_price,
               |    null as count_dsp_floor_price,
               |    null as sum_ssp_floor_price,
               |    null as imp,
               |    null as clk,
               |    max(dsp_final_price) as revenue, -- 只有1个
               |    max(ssp_final_price) as cost, -- 只有1个
               |    null as dsp_req_timeout,
               |    null as dsp_req_parse_error,
               |    null as dsp_req_invalid_ad,
               |    null as dsp_req_no_bid
               |from
               |    ods.adx_saas_dsp_win
               |where
               |    etl_date='${etlDate}' and etl_hour = '$etlHour'
               |group by request_id,dsp_id
               |""".stripMargin)



        val unionDF = joinedDF union impDF union clkDF union sspWinDF union dspWinDF

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
              | max(dsp_req) as dsp_req,
              | sum(dsp_fill_req) as dsp_fill_req,
              | max(dsp_win) as dsp_win,
              | max(ssp_win) as ssp_win,
              | sum(sum_dsp_floor_price) as sum_dsp_floor_price,
              | sum(count_dsp_floor_price) as count_dsp_floor_price,
              | max(sum_ssp_floor_price) as sum_ssp_floor_price,
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
              | request_id,dsp_id
              |""".stripMargin).createOrReplaceTempView("union_table")


    }

}
