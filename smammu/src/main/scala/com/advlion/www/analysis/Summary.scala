package com.advlion.www.analysis

import com.advlion.www.jdbc.MySQL
import org.apache.spark.sql.{SaveMode, SparkSession}

object Summary {
  val url: String = MySQL.url
  val user: String = MySQL.user
  val password: String = MySQL.password
  val driver: String = MySQL.driver


  /**
   *  ssp 数据统计
   * @param spark  spark
   * @param args args
   */

  def sspSummary(spark: SparkSession,args: Array[String]){
    val etlDate = args(0).toString
    val etlHour = args(1).toString
    val etlTime = etlDate + " " + etlHour + ":00:00"
    val sspResult = "stat_hour"
    println(etlDate)
    println(etlHour)
    val sspResultDF = spark.sql(
      s"""select t2.dev_id,
         |		t1.app_id as media_id,
         |		t1.adsolt_id as adslocation_id,
         |		case when nvl(t1.dsp_id,-1)='' then -1 else nvl(t1.dsp_id,-1) end channel_id,
         |		'n' is_upload,
         |		'$etlDate' time,
         |		'$etlTime' as hour,
         |		'yes' del,
         |		t1.show,
         |		t1.click,
         |		t1.valid_pv,
         |		t1.filter_req,
         |		t1.adv_back,
         |		t1.channel_uv,
         |		0 price
         |from
         |	(
         |		select	a.app_id,
         |				a.adsolt_id,
         |				a.dsp_id,
         |				sum(a.show) show,
         |				sum(a.click) click,
         |				sum(a.valid_pv) valid_pv,
         |				sum(a.filter_req) filter_req,
         |				sum(a.adv_back) adv_back,
         |				sum(a.channel_uv) channel_uv
         |	from (
         |			select 	app_id,
         |					adsolt_id,
         |					case when is_default='1' then '-1' else dsp_id end dsp_id,
         |					sum(case when is_default='0' then 1 else 0 end) show,
         |					0 click,
         |					0 valid_pv,
         |					0 filter_req,
         |					0 adv_back,
         |					0 channel_uv
         |			from 	ods.ods_smammu_ssp_imp
         |			where 	etl_date='$etlDate'
         |			and 	etl_hour='$etlHour'
         |			and  	adsolt_id>=10000
         |			group 	by app_id,adsolt_id ,case when is_default='1' then '-1' else dsp_id end
         |			union 	all
         |			select 	app_id,
         |					adsolt_id,
         |					case when is_default='1' then '-1' else dsp_id end dsp_id,
         |					0 show,
         |					sum(case when is_default='0' then 1 else 0 end) click,
         |					0 valid_pv,
         |					0 filter_req,
         |					0 adv_back,
         |					0 channel_uv
         |			from 	ods.ods_smammu_ssp_clk
         |      group 	by app_id,adsolt_id,case when is_default='1' then '-1' else dsp_id end
         |			union 	all
         |			select 	app_id,
         |					adsolt_id,
         |					'-1' dsp_id,
         |					0 show,
         |					0 click,
         |					count(case when is_sdk='0' then concat(nvl(imei,''),nvl(idfa,'')) end) valid_pv,
         |					0 filter_req,
         |					0 adv_back,
         |					0 channel_uv
         |			from 	ods.ods_smammu_media_req
         |			where 	etl_date='$etlDate'
         |			and 	etl_hour='$etlHour'
         |			and 	adsolt_id>=10000
         |			group 	by app_id,adsolt_id
         |			union   all
         |			select 	app_id,
         |					adsolt_id,
         |					'-1' dsp_id,
         |					0 show,
         |					0 click,
         |					0 valid_pv,
         |					count(1) filter_req,
         |					0 adv_back,
         |					0 channel_uv
         |			from 	ods.ods_smammu_media_req_filter
         |			where 	etl_date='$etlDate'
         |			and 	etl_hour='$etlHour'
         |			and 	adsolt_id>=10000
         |			group 	by app_id,adsolt_id
         |			union 	all
         |			select 	app_id,
         |					adsolt_id,
         |					dsp_id,
         |				    0 show,
         |					0 click,
         |					0 valid_pv,
         |					0 filter_req,
         |					count(1) adv_back,
         |					0 channel_uv
         |			from 	ods.ods_smammu_media_resp
         |			where 	etl_date='$etlDate'
         |			and 	etl_hour='$etlHour'
         |			and 	status_code='0'
         |			and 	adsolt_id>=10000
         |			group 	by app_id,adsolt_id,dsp_id
         |			union 	all
         |			select 	app_id,
         |					adsolt_id,
         |					dsp_id,
         |					0 show,
         |					0 click,
         |					0 valid_pv,
         |					0 filter_req,
         |					0 adv_back,
         |					count(1) channel_uv
         |			from 	ods.ods_smammu_dsp_resp
         |			where 	etl_date = '$etlDate'
         |			and 	etl_hour = '$etlHour'
         |			and  	adsolt_id>=10000
         |			group 	by app_id,adsolt_id,dsp_id
         |		) a
         |		group by a.app_id,a.dsp_id,a.adsolt_id
         |	) t1
         |	left  join media t2
         |	on 		t1.app_id = t2.id
         |	where 	t1.app_id is not null and t1.adsolt_id is not null and t1.dsp_id is not null
         |""".stripMargin)
//    sspResultDF.show()
    sspResultDF.write.mode(SaveMode.Append)
//    sspResultDF.write.mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("driver",driver)
      .option("dbtable",sspResult)
      .save()
  }

  /**
   * 竞价数据统计
   * @param spark args
   * @param args args
   */
  def bidSummary(spark: SparkSession,args: Array[String]): Unit ={
    val etlDate = args(0).toString
    val etlHour = args(1).toString
    val etlTime = etlDate + " " + etlHour + ":00:00"
    val bidResult = "stat_hour_dsp"
    val bidResultDF = spark.sql(
      s"""
         |select	p.aduser_id,
         |		m.split_plan_id as plan_id,
         |		m.creative_id,
         |		a.dev_id,
         |		a.media_id,
         |		m.adslot_id as adslocation_id,
         |		m.channel_id,
         |		m.req,
         |		m.bid,
         |		m.show,
         |		m.click,
         |		m.cost,
         |		'$etlDate' as time,
         |		'$etlTime' as hour
         |from
         |	(
         |		select 	t.split_plan_id,
         |				t.creative_id,
         |				t.adslot_id,
         |				t.channel_id,
         |				sum(t.req) req,
         |				sum(t.bid) bid,
         |				sum(t.show) show,
         |				sum(t.click) click,
         |				sum(t.cost) cost
         |		from
         |		(
         |			select	split_plan_id
         |					,'-1'	as creative_id
         |					,adslot_id
         |					,79	as channel_id
         |					,count(request_id) as req
         |					,0 as bid
         |					,0 as show
         |					,0 as click
         |					,0 as cost
         |			from 	ods.ods_smammu_req
         |			lateral view explode(split(plan_id, ',')) req_plan as  split_plan_id
         |			where 	etl_date='$etlDate'
         |			and		etl_hour='$etlHour'
         |			group	by split_plan_id,adslot_id
         |			union	all
         |			select	split_plan_id
         |					,creative_id
         |					,adslot_id
         |					,79	as channel_id
         |					,0	as req
         |					,count(request_id) as bid
         |					,0 as show
         |					,0 as click
         |					,0 as cost
         |			from 	ods.ods_smammu_bid
         |			lateral view explode(split(plan_id, ',')) split_imp as  split_plan_id
         |			where 	etl_date='$etlDate'
         |			and		etl_hour='$etlHour'
         |			group	by split_plan_id,creative_id,adslot_id
         |			union	all
         |			select	split_plan_id
         |					,creative_id
         |					,adslot_id
         |					,79	as channel_id
         |					,0	as req
         |					,0	as bid
         |					,count(request_id) as show
         |					,0 as click
         |					,sum(pay_price) as cost
         |			from 	ods.ods_smammu_imp
         |			lateral view explode(split(plan_id, ',')) split_bid as  split_plan_id
         |			where 	etl_date='$etlDate'
         |			and		etl_hour='$etlHour'
         |			group	by split_plan_id,creative_id,adslot_id
         |			union   all
         |			select	split_plan_id
         |					,creative_id
         |					,adslot_id
         |					,79	as channel_id
         |					,0	as req
         |					,0	as bid
         |					,0	as show
         |					,count(request_id) as click
         |					,sum(pay_price)  as cost
         |			from 	ods.ods_smammu_clk
         |			lateral view explode(split(plan_id, ',')) split_clk as  split_plan_id
         |			where 	etl_date='$etlDate'
         |			and		etl_hour='$etlHour'
         |			group	by split_plan_id,creative_id,adslot_id
         |		) t
         |		group	by t.split_plan_id,t.creative_id,t.adslot_id,t.channel_id
         |	)	m
         |	left	join plan p
         |	on 		m.split_plan_id = p.id
         |	left	join adslocation a
         |	on 		m.adslot_id = a.id
         |
         |"""
      .stripMargin)

//    bidResultDF.show()
    bidResultDF.write.mode(SaveMode.Append)
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("driver",driver)
      .option("dbtable",bidResult)
      .save()
  }
}
