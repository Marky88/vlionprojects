package com.advlion.www.load

import org.apache.spark.sql.SparkSession

object LoadHive {
  def readHive(spark:SparkSession,args: Array[String]): Unit ={
    val etlDate = args(0).toString
    val etlHour = args(1).toString
    val etlTime = etlDate + " " + etlHour + ":00:00"

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
         |""".stripMargin)

//    println("统计的行数"+summaryDF.count())
//    summaryDF.show()
//    summaryDF.write.format("csv").save("/tmp/test/aaa")


    summaryDF.createOrReplaceTempView("summary")
  }
}
