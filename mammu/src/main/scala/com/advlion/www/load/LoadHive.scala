package com.advlion.www.load

import org.apache.spark.sql.SparkSession

object LoadHive {
  def readHive(spark:SparkSession,args: Array[String]): Unit ={
    val etlDate = args(0).toString
    val etlHour = args(1).toString
    val etlTime = etlDate + " " + etlHour + ":00:00"

   /*
     //之前的需求
    val summaryDF = spark.sql(
      s"""
         |select '$etlDate' as time,
         |		'$etlTime' as hour,
         |		a.split_plan_id as plan_id,
         |		a.creative_id as creative_id,
         |		a.adslot_id,
         |		a.channel_id,
         |		sum(a.req) req,
         |		sum(a.bid) bid,
         |		sum(a.show) show,
         |		sum(a.click) click,
         |		sum(a.cost) cost
         |from
         |(
         |	select	split_plan_id
         |			,-1	as creative_id
         |			,adslot_id
         |			,79	as channel_id
         |			,count(request_id) as req
         |			,0 as bid
         |			,0 as show
         |			,0 as click
         |			,0 as cost
         |	from 	ods.req_bid_imp_clk_51
         |	lateral view explode(split(plan_id, ',')) split_51 as  split_plan_id
         |	where 	etl_date='$etlDate'
         |	and		etl_hour='$etlHour'
         |	group	by split_plan_id,adslot_id
         |	union	all
         |	select	split_plan_id
         |			,creative_id
         |			,adslot_id
         |			,79	as channel_id
         |			,0	as req
         |			,count(request_id) as bid
         |			,0 as show
         |			,0 as click
         |			,0 as cost
         |	from 	ods.req_bid_imp_clk_52
         |	lateral view explode(split(plan_id, ',')) split_52 as  split_plan_id
         |	where 	etl_date='$etlDate'
         |	and		etl_hour='$etlHour'
         |	group	by split_plan_id,creative_id,adslot_id
         |	union	all
         |	select	split_plan_id
         |			,creative_id
         |			,adslot_id
         |			,79	as channel_id
         |			,0	as req
         |			,0	as bid
         |			,count(request_id) as show
         |			,0 as click
         |			,sum(pay_price) as cost
         |	from 	ods.req_bid_imp_clk_53
         |	lateral view explode(split(plan_id, ',')) split_53 as  split_plan_id
         |	where 	etl_date='$etlDate'
         |	and		etl_hour='$etlHour'
         |	group	by split_plan_id,creative_id,adslot_id
         |	union   all
         |	select	split_plan_id
         |			,creative_id
         |			,adslot_id
         |			,79	as channel_id
         |			,0	as req
         |			,0	as bid
         |			,0	as show
         |			,count(request_id) as click
         |			,sum(pay_price)  as cost
         |	from 	ods.req_bid_imp_clk_54
         |	lateral view explode(split(plan_id, ',')) split_54 as  split_plan_id
         |	where 	etl_date='$etlDate'
         |	and		etl_hour='$etlHour'
         |	group	by split_plan_id,creative_id,adslot_id
         |) a
         |group by a.split_plan_id,a.creative_id,a.adslot_id,a.channel_id
         |""".stripMargin)*/


    val summaryDF = spark.sql(
      s"""
         |select
         |      '$etlDate' as etl_date,
         |		'$etlHour' as etl_hour,
         |      '$etlTime' as etl_date_hour,
         |		a.split_plan_id as plan_id,
         |		a.creative_id as creative_id,
         |		a.adslot_id,
         |      a.geo,
         |      a.adsolt_group_name,
         |		a.channel_id,
         |		sum(a.req) req,
         |		sum(a.bid) bid,
         |		sum(a.imp) imp,
         |		sum(a.clk) clk,
         |      sum(a.ocpx) ocpx,
         |		sum(a.bid_real_cost)  as bid_real_cost,
         |      sum(a.imp_real_cost)  as imp_real_cost,
         |      sum(a.alp_1) as alp_1,
         |      sum(a.alp_2) as alp_2,
         |      sum(a.alp_3) as alp_3,
         |      sum(a.alp_4) as alp_4
         |from
         |(
         |	select	split_plan_id
         |			,-1	as creative_id
         |			,adslot_id
         |          ,geo
         |          ,adsolt_group_name
         |			,79	as channel_id
         |			,count(request_id) as req
         |			,0 as bid
         |			,0 as imp
         |			,0 as clk
         |          ,0 as ocpx
         |			,0 as bid_real_cost
         |          ,0 as imp_real_cost
         |          ,0  as alp_1
         |          ,0  as alp_2
         |          ,0  as alp_3
         |          ,0  as alp_4
         |	from 	ods.req_bid_imp_clk_51
         |	lateral view explode(split(plan_id, ',')) split_51 as  split_plan_id
         |	where 	etl_date='$etlDate'
         |	and		etl_hour='$etlHour'
         |	group	by split_plan_id,adslot_id,geo,adsolt_group_name
         |	union	all
         |	select	split_plan_id
         |			,creative_id
         |			,adslot_id
         |          ,geo
         |          ,adsolt_group_name
         |			,79	as channel_id
         |			,0	as req
         |			,count(request_id) as bid
         |			,0 as imp
         |			,0 as clk
         |          ,0 as ocpx
         |			,sum(real_bid_price) as bid_real_cost
         |          ,0 as imp_real_cost
         |          ,0  as alp_1
         |          ,0  as alp_2
         |          ,0  as alp_3
         |          ,0  as alp_4
         |	from 	ods.req_bid_imp_clk_52
         |	lateral view explode(split(plan_id, ',')) split_52 as  split_plan_id
         |	where 	etl_date='$etlDate'
         |	and		etl_hour='$etlHour'
         |	group	by split_plan_id,creative_id,adslot_id,geo,adsolt_group_name
         |	union	all
         |	select	split_plan_id
         |			,creative_id
         |			,adslot_id
         |          ,geo
         |          ,adsolt_group_name
         |			,79	as channel_id
         |			,0	as req
         |			,0	as bid
         |			,count(request_id) as imp
         |			,0 as clk
         |          ,0 as ocpx
         |			,0 as bid_real_cost
         |          ,sum(real_pay_price) as imp_real_cost
         |          ,0  as alp_1
         |          ,0  as alp_2
         |          ,0  as alp_3
         |          ,0  as alp_4
         |	from 	ods.req_bid_imp_clk_53
         |	lateral view explode(split(plan_id, ',')) split_53 as  split_plan_id
         |	where 	etl_date='$etlDate'
         |	and		etl_hour='$etlHour'
         |	group	by split_plan_id,creative_id,adslot_id,geo,adsolt_group_name
         |	union   all
         |	select	split_plan_id
         |			,creative_id
         |			,adslot_id
         |          ,geo
         |          ,adsolt_group_name
         |			,79	as channel_id
         |			,0	as req
         |			,0	as bid
         |			,0	as imp
         |			,count(request_id) as clk
         |          ,0  as ocpx
         |			,0  as bid_real_cost
         |          ,0  as imp_real_cost
         |          ,0  as alp_1
         |          ,0  as alp_2
         |          ,0  as alp_3
         |          ,0  as alp_4
         |	from 	ods.req_bid_imp_clk_54
         |	lateral view explode(split(plan_id, ',')) split_54 as  split_plan_id
         |	where 	etl_date='$etlDate'
         |	and		etl_hour='$etlHour'
         |	group	by split_plan_id,creative_id,adslot_id,geo,adsolt_group_name
         |  union   all
         |  select  split_plan_id,
         |          creative_id,
         |          adslot_id,
         |          geo,
         |          adsolt_group_name,
         |          79	as channel_id,
         |          0 as req,
         |          0 as bid,
         |          0 as imp,
         |          0 as clk,
         |          count(request_id) as ocpx,
         |          0 as bid_real_cost,
         |          0 as imp_real_cost,
         |          count(if(ocpx_tag='alp_1',1,null)) as alp_1,
         |          count(if(ocpx_tag='alp_2',1,null)) as alp_2,
         |          count(if(ocpx_tag='alp_3',1,null)) as alp_3,
         |          count(if(ocpx_tag='alp_4',1,null)) as alp_4
         |  from    ods.req_bid_imp_clk_55
         |  lateral view explode(split(plan_id,',')) split_55 as  split_plan_id
         |  where   etl_date='$etlDate'  and etl_hour='$etlHour'
         |  group by
         |         split_plan_id,creative_id,adslot_id,geo,adsolt_group_name
         |) a
         |group by a.split_plan_id,a.creative_id,a.adslot_id,a.geo,a.adsolt_group_name,a.channel_id
         |""".stripMargin)

    summaryDF.createOrReplaceTempView("summary_tmp")


/*      print("统计行数:"+ summaryDF.count())
      summaryDF.show(20)

      summaryDF.write.format("csv").save("/tmp/test/abc.csv")*/











//    println("统计的行数"+summaryDF.count())
//    summaryDF.show()
//    summaryDF.write.format("csv").save("/tmp/test/aaa")


  //  summaryDF.createOrReplaceTempView("summary")
  }
}
