package com.advlion.www.load

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object LoadHive {
  def readHive(spark:SparkSession,args: Array[String]): Unit ={
    val etlDate = args(0).toString
    val etlHour = args(1).toString
    println(etlDate)
    println(etlHour)
    // 301的日志
    val sspImpDF = spark.sql(
      s"""
         |select app_id,
         |		case when is_default='1' then '-1' else dsp_id end dsp_id,
         |		adsolt_id,
         |		0 valid_uv,
         |		0 valid_pv,
         |		0 sdk_uv,
         |		0 sdk_pv,
         |		0 filter_req,
         |		0 adv_back,
         |		sum(case when is_default='1' then 1 else 0 end) def_show,
         |		0 def_click,
         |		sum(case when is_default='0' then 1 else 0 end) show,
         |		0 click,
         |		0 dpnum,
         |		0 clkdpnum,
         |		0 channel_uv,
         |		0 channel_pv,
         |		0 send_dsp_res,
         |		0 down_start ,
         |    0 down_complate ,
         |    0 install_start ,
         |    0 install_complate ,
         |    0 app_start ,
         |    0 v_cache_done ,
         |    0 v_play_start ,
         |    0 v_play_done,
         |     0 rjcost
         |from ods.ods_ssp_imp
         |where etl_date = '$etlDate'
         |and etl_hour ='$etlHour'
         |group by app_id,adsolt_id ,case when is_default='1' then '-1' else dsp_id end
         |""".stripMargin)
//      .persist(StorageLevel.DISK_ONLY)

    val sspClkDF = spark.sql(
      s"""
        |select	app_id,
        |		case when is_default='1' then '-1' else dsp_id end dsp_id,
        |		adsolt_id,
        |		0 valid_uv,
        |		0 valid_pv,
        |		0 sdk_uv,
        |		0 sdk_pv,
        |		0 filter_req,
        |		0 adv_back,
        |		0 def_show,
        |		sum(case when is_default='1' then 1 else 0 end) def_click,
        |		0 show,
        |		sum(case when is_default='0' then 1 else 0 end) click,
        |		0 dpnum,
        |		sum(case when deeplink='1' then 1 else 0 end) clkdpnum,
        |		0 channel_uv,
        |		0 channel_pv,
        |		0 send_dsp_res,
        |	0 down_start ,
        |	0 down_complate ,
        |	0 install_start ,
        |	0 install_complate ,
        |	0 app_start ,
        |	0 v_cache_done ,
        |	0 v_play_start ,
        |	0 v_play_done,
        |  0 rjcost
        |from ods.ods_ssp_clk
        |where etl_date = '$etlDate'
        |and etl_hour ='$etlHour'
        |group 	by app_id,adsolt_id,case when is_default='1' then '-1' else dsp_id end
        |""".stripMargin)
    //     .persist(StorageLevel.DISK_ONLY)

    val mediaReqDF = spark.sql(
      s"""
        |select app_id,
        |		'-1' dsp_id,
        |		adsolt_id,
        |		count(distinct case when is_sdk='0' then concat(nvl(imei,''),nvl(idfa,'')) end) valid_uv,
        |		count(case when is_sdk='0' then 1 end) valid_pv,
        |		count(distinct case when is_sdk='1' then concat(nvl(imei,''),nvl(idfa,'')) end) sdk_uv,
        |		count(case when is_sdk='1' then 1 end) sdk_pv,
        |		0 filter_req,
        |		0 adv_back,
        |		0 def_show,
        |		0 def_click,
        |		0 show,
        |		0 click,
        |		0 dpnum,
        |		0 clkdpnum,
        |		0 channel_uv,
        |		0 channel_pv,
        |		0 send_dsp_res,
        |	0 down_start ,
        |	0 down_complate ,
        |	0 install_start ,
        |	0 install_complate ,
        |	0 app_start ,
        |	0 v_cache_done ,
        |	0 v_play_start ,
        |	0 v_play_done,
        |  0 rjcost
        |from ods.ods_media_req
        |where etl_date = '$etlDate'
        |and etl_hour ='$etlHour'
        |group by app_id,adsolt_id
        |""".stripMargin)
    // .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val mediaRespDF = spark.sql(
      s"""
        |select app_id,
        |		dsp_id,
        |		adsolt_id,
        |		0 valid_uv,
        |		0 valid_pv,
        |		0 sdk_uv,
        |		0 sdk_pv,
        |		0 filter_req,
        |		count(1) adv_back,
        |		0 def_show,
        |		0 def_click,
        |		0 show,
        |		0 click,
        |		0 dpnum,
        |		0 clkdpnum,
        |		0 channel_uv,
        |		0 channel_pv,
        |		0 send_dsp_res,
        |	0 down_start ,
        |	0 down_complate ,
        |	0 install_start ,
        |	0 install_complate ,
        |	0 app_start ,
        |	0 v_cache_done ,
        |	0 v_play_start ,
        |	0 v_play_done,
        |  0 rjcost
        |from ods.ods_media_resp
        |where etl_date = '$etlDate'
        |and etl_hour ='$etlHour'
        |and 	status_code='0'
        |group 	by app_id,adsolt_id,dsp_id
        |""".stripMargin)
    //   .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val mediaReqFilterDF = spark.sql(
      s"""
        |select app_id,
        |		'-1' dsp_id,
        |		adsolt_id,
        |		0 valid_uv,
        |		0 valid_pv,
        |		0 sdk_uv,
        |		0 sdk_pv,
        |		count(1) filter_req,
        |		0 adv_back,
        |		0 def_show,
        |		0 def_click,
        |		0 show,
        |		0 click,
        |		0 dpnum,
        |		0 clkdpnum,
        |		0 channel_uv,
        |		0 channel_pv,
        |		0 send_dsp_res,
        |	0 down_start ,
        |	0 down_complate ,
        |	0 install_start ,
        |	0 install_complate ,
        |	0 app_start ,
        |	0 v_cache_done ,
        |	0 v_play_start ,
        |	0 v_play_done,
        |  0 rjcost
        |from ods.ods_media_req_filter
        |where etl_date = '$etlDate'
        |and etl_hour ='$etlHour'
        |group 	by app_id,adsolt_id
        |""".stripMargin)
    //   .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 202日志 收到的 dsp/ssp 返回
    val dspRespDF = spark.sql(
      s"""
        |select app_id,
        |		dsp_id,
        |		adsolt_id,
        |		0 valid_uv,
        |		0 valid_pv,
        |		0 sdk_uv,
        |		0 sdk_pv,
        |		0 filter_req,
        |		0 adv_back,
        |		0 def_show,
        |		0 def_click,
        |		0 show,
        |		0 click,
        |		0 dpnum,
        |		0 clkdpnum,
        |		count(1) channel_uv,
        |		count(1) channel_pv,
        |		0 send_dsp_res,
        |	0 down_start ,
        |	0 down_complate ,
        |	0 install_start ,
        |	0 install_complate ,
        |	0 app_start ,
        |	0 v_cache_done ,
        |	0 v_play_start ,
        |	0 v_play_done,
        |  0 rjcost
        |from ods.ods_dsp_resp
        |where etl_date = '$etlDate'
        |and etl_hour ='$etlHour'
        |group 	by app_id,adsolt_id,dsp_id
        |""".stripMargin)
    //  .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val sspDpDF = spark.sql(
      s"""
        |select app_id,
        |		dsp_id,
        |		adsolt_id,
        |		0 valid_uv,
        |		0 valid_pv,
        |		0 sdk_uv,
        |		0 sdk_pv,
        |		0 filter_req,
        |		0 adv_back,
        |		0 def_show,
        |		0 def_click,
        |		0 show,
        |		0 click,
        |		count(1) dpnum,
        |		0 clkdpnum,
        |		0 channel_uv,
        |		0 channel_pv,
        |		0 send_dsp_res,
        |	0 down_start ,
        |	0 down_complate ,
        |	0 install_start ,
        |	0 install_complate ,
        |	0 app_start ,
        |	0 v_cache_done ,
        |	0 v_play_start ,
        |	0 v_play_done,
        |  0 rjcost
        |from ods.ods_ssp_dp
        |where etl_date = '$etlDate'
        |and etl_hour ='$etlHour'
        |group by app_id,adsolt_id,dsp_id
        |""".stripMargin)
    // .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val sendDspResDF = spark.sql(
      s"""
        |select app_id,
        |		dsp_id,
        |		adsolt_id,
        |		0 valid_uv,
        |		0 valid_pv,
        |		0 sdk_uv,
        |		0 sdk_pv,
        |		0 filter_req,
        |		0 adv_back,
        |		0 def_show,
        |		0 def_click,
        |		0 show,
        |		0 click,
        |		0 dpnum,
        |		0 clkdpnum,
        |		0 channel_uv,
        |		0 channel_pv,
        |		count(1) send_dsp_res,
        |	0 down_start ,
        |	0 down_complate ,
        |	0 install_start ,
        |	0 install_complate ,
        |	0 app_start ,
        |	0 v_cache_done ,
        |	0 v_play_start ,
        |	0 v_play_done,
        |  0 rjcost
        |from ods.ods_send_dsp_res
        |where etl_date = '$etlDate'
        |and etl_hour ='$etlHour'
        |group by app_id,adsolt_id,dsp_id
        |""".stripMargin)
    //  .persist(StorageLevel.MEMORY_AND_DISK_SER)

    //新增8张表
    val sspDownStartDF= spark.sql(
      s"""
         |
         |select app_id,
         |		case when is_default='1' then '-1' else dsp_id end dsp_id,
         |		adsolt_id,
         |		0 valid_uv,
         |		0 valid_pv,
         |		0 sdk_uv,
         |		0 sdk_pv,
         |		0 filter_req,
         |		0 adv_back,
         |		0 def_show,
         |		0 def_click,
         |		0 show,
         |		0 click,
         |		0 dpnum,
         |		0 clkdpnum,
         |		0 channel_uv,
         |		0 channel_pv,
         |		0 send_dsp_res,
         |	  count(1) down_start ,
         |	  0 down_complate ,
         |	  0 install_start ,
         |	  0 install_complate ,
         |	  0 app_start ,
         |	  0 v_cache_done ,
         |	  0 v_play_start ,
         |	  0 v_play_done,
         |    0 rjcost
         |from ods.ods_ssp_down_start
         |where etl_date = '$etlDate'
         |and etl_hour ='$etlHour'
         |group by app_id,adsolt_id ,case when is_default='1' then '-1' else dsp_id end
         |
         |
         |""".stripMargin)
    //   .persist(StorageLevel.MEMORY_AND_DISK_SER)


    val sspDownCompleteDF= spark.sql(
      s"""
        |select app_id,
        |		case when is_default='1' then '-1' else dsp_id end dsp_id,
        |		adsolt_id,
        |		0 valid_uv,
        |		0 valid_pv,
        |		0 sdk_uv,
        |		0 sdk_pv,
        |		0 filter_req,
        |		0 adv_back,
        |		0 def_show,
        |		0 def_click,
        |		0 show,
        |		0 click,
        |		0 dpnum,
        |		0 clkdpnum,
        |		0 channel_uv,
        |		0 channel_pv,
        |		0 send_dsp_res,
        |	  0 down_start ,
        |	  count(1) down_complate ,
        |	  0 install_start ,
        |	  0 install_complate ,
        |	  0 app_start ,
        |	  0 v_cache_done ,
        |	  0 v_play_start ,
        |	  0 v_play_done,
        |    0 rjcost
        |from ods.ods_ssp_down_complete
        |where etl_date = '$etlDate'
        |and etl_hour ='$etlHour'
        |group by app_id,adsolt_id ,case when is_default='1' then '-1' else dsp_id end
        |
        |""".stripMargin)//.persist(StorageLevel.MEMORY_AND_DISK)

    val sspInstallStartDF= spark.sql(
      s"""
        |select app_id,
        |		case when is_default='1' then '-1' else dsp_id end dsp_id,
        |		adsolt_id,
        |		0 valid_uv,
        |		0 valid_pv,
        |		0 sdk_uv,
        |		0 sdk_pv,
        |		0 filter_req,
        |		0 adv_back,
        |		0 def_show,
        |		0 def_click,
        |		0 show,
        |		0 click,
        |		0 dpnum,
        |		0 clkdpnum,
        |		0 channel_uv,
        |		0 channel_pv,
        |		0 send_dsp_res,
        |	  0 down_start ,
        |	  0 down_complate ,
        |	  count(1)  install_start ,
        |	  0 install_complate ,
        |	  0 app_start ,
        |	  0 v_cache_done ,
        |	  0 v_play_start ,
        |	  0 v_play_done,
        |    0 rjcost
        |from ods.ods_ssp_install_start
        |where etl_date = '$etlDate'
        |and etl_hour ='$etlHour'
        |group by app_id,adsolt_id ,case when is_default='1' then '-1' else dsp_id end
        |
        |""".stripMargin)//.persist(StorageLevel.MEMORY_AND_DISK)

    val sspInstallDoneDF=spark.sql(
      s"""
         |select app_id,
         |		case when is_default='1' then '-1' else dsp_id end dsp_id,
         |		adsolt_id,
         |		0 valid_uv,
         |		0 valid_pv,
         |		0 sdk_uv,
         |		0 sdk_pv,
         |		0 filter_req,
         |		0 adv_back,
         |		0 def_show,
         |		0 def_click,
         |		0 show,
         |		0 click,
         |		0 dpnum,
         |		0 clkdpnum,
         |		0 channel_uv,
         |		0 channel_pv,
         |		0 send_dsp_res,
         |	  0 down_start ,
         |	  0 down_complate ,
         |	  0 install_start ,
         |	  count(1)  install_complate ,
         |	  0 app_start ,
         |	  0 v_cache_done ,
         |	  0 v_play_start ,
         |	  0 v_play_done,
         |    0 rjcost
         |from ods.ods_ssp_install_done
         |where etl_date = '$etlDate'
         |and etl_hour ='$etlHour'
         |group by app_id,adsolt_id ,case when is_default='1' then '-1' else dsp_id end
         |
         |""".stripMargin)//.persist(StorageLevel.MEMORY_AND_DISK)

    val sspAppStartDF=spark.sql(
      s"""
         |select app_id,
         |		case when is_default='1' then '-1' else dsp_id end dsp_id,
         |		adsolt_id,
         |		0 valid_uv,
         |		0 valid_pv,
         |		0 sdk_uv,
         |		0 sdk_pv,
         |		0 filter_req,
         |		0 adv_back,
         |		0 def_show,
         |		0 def_click,
         |		0 show,
         |		0 click,
         |		0 dpnum,
         |		0 clkdpnum,
         |		0 channel_uv,
         |		0 channel_pv,
         |		0 send_dsp_res,
         |	  0 down_start ,
         |	  0 down_complate ,
         |	  0 install_start ,
         |	  0 install_complate ,
         |	  count(1)  app_start ,
         |	  0 v_cache_done ,
         |	  0 v_play_start ,
         |	  0 v_play_done,
         |    0 rjcost
         |from ods.ods_ssp_app_start
         |where etl_date = '$etlDate'
         |and etl_hour ='$etlHour'
         |group by app_id,adsolt_id ,case when is_default='1' then '-1' else dsp_id end
         |
         |""".stripMargin)//.persist(StorageLevel.MEMORY_AND_DISK)

    val sspVCacheDoneDF=spark.sql(
      s"""
         |select app_id,
         |		case when is_default='1' then '-1' else dsp_id end dsp_id,
         |		adsolt_id,
         |		0 valid_uv,
         |		0 valid_pv,
         |		0 sdk_uv,
         |		0 sdk_pv,
         |		0 filter_req,
         |		0 adv_back,
         |		0 def_show,
         |		0 def_click,
         |		0 show,
         |		0 click,
         |		0 dpnum,
         |		0 clkdpnum,
         |		0 channel_uv,
         |		0 channel_pv,
         |		0 send_dsp_res,
         |	  0 down_start ,
         |	  0 down_complate ,
         |	  0 install_start ,
         |	  0 install_complate ,
         |	  0 app_start ,
         |	  count(1) v_cache_done ,
         |	  0 v_play_start ,
         |	  0 v_play_done,
         |    0 rjcost
         |from ods.ods_ssp_v_cache_done
         |where etl_date = '$etlDate'
         |and etl_hour ='$etlHour'
         |group by app_id,adsolt_id ,case when is_default='1' then '-1' else dsp_id end
         |
         |""".stripMargin)//.persist(StorageLevel.MEMORY_AND_DISK)

    val sspVPlayStartDF=spark.sql(
      s"""
         |select app_id,
         |		case when is_default='1' then '-1' else dsp_id end dsp_id,
         |		adsolt_id,
         |		0 valid_uv,
         |		0 valid_pv,
         |		0 sdk_uv,
         |		0 sdk_pv,
         |		0 filter_req,
         |		0 adv_back,
         |		0 def_show,
         |		0 def_click,
         |		0 show,
         |		0 click,
         |		0 dpnum,
         |		0 clkdpnum,
         |		0 channel_uv,
         |		0 channel_pv,
         |		0 send_dsp_res,
         |	  0 down_start ,
         |	  0 down_complate ,
         |	  0 install_start ,
         |	  0 install_complate ,
         |	  0 app_start ,
         |	  0 v_cache_done ,
         |	  count(1) v_play_start ,
         |	  0 v_play_done,
         |    0 rjcost
         |from ods.ods_ssp_v_play_start
         |where etl_date = '$etlDate'
         |and etl_hour ='$etlHour'
         |group by app_id,adsolt_id ,case when is_default='1' then '-1' else dsp_id end
         |
         |""".stripMargin)//.persist(StorageLevel.MEMORY_AND_DISK)


    val sspVPlayDoneDF=spark.sql(
      s"""
         |select app_id,
         |		case when is_default='1' then '-1' else dsp_id end dsp_id,
         |		adsolt_id,
         |		0 valid_uv,
         |		0 valid_pv,
         |		0 sdk_uv,
         |		0 sdk_pv,
         |		0 filter_req,
         |		0 adv_back,
         |		0 def_show,
         |		0 def_click,
         |		0 show,
         |		0 click,
         |		0 dpnum,
         |		0 clkdpnum,
         |		0 channel_uv,
         |		0 channel_pv,
         |		0 send_dsp_res,
         |	  0 down_start ,
         |	  0 down_complate ,
         |	  0 install_start ,
         |	  0 install_complate ,
         |	  0 app_start ,
         |	  0 v_cache_done ,
         |	  0 v_play_start ,
         |	  count(1) v_play_done,
         |   0 rjcost
         |from ods.ods_ssp_v_play_done
         |where etl_date = '$etlDate'
         |and etl_hour ='$etlHour'
         |group by app_id,adsolt_id ,case when is_default='1' then '-1' else dsp_id end
         |
         |""".stripMargin)//.persist(StorageLevel.MEMORY_AND_DISK)

    //20210129新增
    val sspWinDF = spark.sql(
      s"""
         |select
         |app_id,
         |dsp_id,
         |adsolt_id,
         |0 valid_uv,
         |0 valid_pv,
         |0 sdk_uv,
         |0 sdk_pv,
         |0 filter_req,
         |0 adv_back,
         |0 def_show,
         |0 def_click,
         |0 show,
         |0 click,
         |0 dpnum,
         |0 clkdpnum,
         |0 channel_uv,
         |0 channel_pv,
         |0 send_dsp_res,
         |0 down_start ,
         |0 down_complate ,
         |0 install_start ,
         |0 install_complate ,
         |0 app_start ,
         |0 v_cache_done ,
         |0 v_play_start ,
         |0 v_play_done,
         |sum(cast (if(dsp_price is null or dsp_price='' ,null,dsp_price) as double)) as rjcost
         |from
         |ods.ods_ssp_win
         |where etl_date = '$etlDate'
         |and etl_hour ='$etlHour'
         |group by app_id,adsolt_id ,dsp_id
         |""".stripMargin)

    sspImpDF.createOrReplaceTempView("sspImp")
    sspClkDF.createOrReplaceTempView("sspClk")
    mediaReqDF.createOrReplaceTempView("mediaReq")
    mediaRespDF.createOrReplaceTempView("mediaResp")
    mediaReqFilterDF.createOrReplaceTempView("mediaReqFilter")
    dspRespDF.createOrReplaceTempView("dspResp")
    sspDpDF.createOrReplaceTempView("sspDp")
    sendDspResDF.createOrReplaceTempView("sendDspRes")

    sspDownStartDF.createOrReplaceTempView("sspDownStart")
    sspDownCompleteDF.createOrReplaceTempView("sspDownComplete")
    sspInstallStartDF.createOrReplaceTempView("sspInstallStart")
    sspInstallDoneDF.createOrReplaceTempView("sspInstallDone")
    sspAppStartDF.createOrReplaceTempView("sspAppStart")
    sspVCacheDoneDF.createOrReplaceTempView("sspVCacheDone")
    sspVPlayStartDF.createOrReplaceTempView("sspVPlayStart")
    sspVPlayDoneDF.createOrReplaceTempView("sspVPlayDone")
    sspWinDF.createOrReplaceTempView("sspWin")

    val sql=
      s"""
         |
         |select
         |app_id,dsp_id,adsolt_id,
         |sum(valid_uv)          as  valid_uv              ,
         |sum(valid_pv)          as  valid_pv              ,
         |sum(sdk_uv)            as  sdk_uv                ,
         |sum(sdk_pv)            as  sdk_pv                ,
         |sum(filter_req)        as  filter_req            ,
         |sum(adv_back)          as  adv_back              ,
         |sum(def_show)          as  def_show              ,
         |sum(def_click)         as  def_click             ,
         |sum(show)              as  show                  ,
         |sum(click)             as  click                 ,
         |sum(dpnum)             as  dpnum                 ,
         |sum(clkdpnum)          as  clkdpnum              ,
         |sum(channel_uv)        as  channel_uv            ,
         |sum(channel_pv)        as  channel_pv            ,
         |sum(send_dsp_res)      as  send_dsp_res          ,
         |sum(down_start)        as  down_start            ,
         |sum(down_complate)     as  down_complate         ,
         |sum(install_start)     as  install_start         ,
         |sum(install_complate)  as  install_complate      ,
         |sum(app_start)         as  app_start             ,
         |sum(v_cache_done)      as  v_cache_done          ,
         |sum(v_play_start)      as  v_play_start          ,
         |sum(v_play_done)       as  v_play_done           ,
         |sum(rjcost)            as  rjcost
         |
         |from
         |(
         |select * from sspImp
         |union
         |select * from sspClk
         |union
         |select * from mediaReq
         |union
         |select * from mediaResp
         |union
         |select * from mediaReqFilter
         |union
         |select * from dspResp
         |union
         |select * from sspDp
         |union
         |select * from sendDspRes
         |union
         |
         |select * from sspDownStart
         |union
         |select * from sspDownComplete
         |union
         |select * from sspInstallStart
         |union
         |select * from sspInstallDone
         |union
         |select * from sspAppStart
         |union
         |select * from sspVCacheDone
         |union
         |select * from sspVPlayStart
         |union
         |select * from sspVPlayDone
         |union
         |select * from sspWin
         |
         |) t
         |where string(adsolt_id) regexp '^[0-9]+$$'
         |group by app_id,dsp_id,adsolt_id
         |""".stripMargin

    //where adsolt_id regexp '^\\d+$$'

    val unionDF = spark.sql(sql)
    unionDF.persist(StorageLevel.MEMORY_AND_DISK)

    unionDF.createOrReplaceTempView("summary")
//    val unionDF = sspImpDF.union(sspClkDF).union(mediaReqDF).union(mediaRespDF)
//      .union(mediaReqFilterDF).union(dspRespDF).union(sspDpDF).union(sendDspResDF)
//      .union(sspDownStartDF).union(sspDownCompleteDF).union(sspInstallStartDF).union(sspInstallDoneDF)
//      .union(sspAppStartDF).union(sspVCacheDoneDF).union(sspVPlayStartDF).union(sspVPlayDoneDF)
//      .groupBy("app_id", "dsp_id", "adsolt_id")
//      .sum("valid_uv", "valid_pv", "sdk_uv", "sdk_pv", "filter_req",
//        "adv_back", "def_show", "def_click", "show", "click", "dpnum",
//        "clkdpnum", "channel_uv", "channel_pv", "send_dsp_res","down_start","down_complate",
//        "install_start","install_complate","app_start","v_cache_done","v_play_start","v_play_done")
//      .toDF("app_id", "dsp_id", "adsolt_id", "valid_uv", "valid_pv", "sdk_uv",
//        "sdk_pv", "filter_req", "adv_back", "def_show", "def_click", "show", "click",
//        "dpnum", "clkdpnum", "channel_uv", "channel_pv", "send_dsp_res","down_start","down_complate",
//        "install_start","install_complate","app_start","v_cache_done","v_play_start","v_play_done"
//      )
//
//    unionDF.createOrReplaceTempView("summary")
//    unionDF.show()
//    println("unionDF")
  }
  /**
   * 媒体请求日活数据统计
   */
  def hiveDau(spark:SparkSession,args: Array[String]): Unit = {
    val etlDate = args(0).toString
//    val etlHour = args(1).toString
    val dauDF = spark.sql(
      s"""
         |select '$etlDate' time,
         |        app_id,
         |			 '-1' dsp_id,
         |				adsolt_id,
         |				count(distinct case when is_sdk='0' then concat(nvl(imei,''),nvl(idfa,'')) end) valid_uv,
         |				count(case when is_sdk='0' then 1 end) valid_pv,
         |				count(distinct case when is_sdk='1' and os = '1' then concat(nvl(android_id,'')) end) android_sdk_uv,
         |				count(distinct case when is_sdk='1' and os = '2' then concat(nvl(imei,''),nvl(idfa,'')) end) ios_sdk_uv,
         |				count(case when is_sdk='1' and os = '1'  then 1 end) android_sdk_pv,
         |				count(case when is_sdk='1' and os = '2'  then 1 end) ios_sdk_pv
         |from 	  ods.ods_media_req
         |where 	etl_date='$etlDate'
         |group 	by app_id,adsolt_id
         |""".stripMargin)
    dauDF.createOrReplaceTempView("dau")

  }
}
