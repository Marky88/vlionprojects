package com.vlion.adx_saas_dsp.analysis

import java.sql.DriverManager

import com.vlion.adx_saas_dsp.jdbc.MySQL
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
 * @description:
 * @author: malichun
 * @time: 2021/6/9/0009 15:58
 *
 */
object OfflineStatistics {
    val url: String = MySQL.url
    val user: String  = MySQL.user
    val password: String  = MySQL.password
    val driver: String  = MySQL.driver


    //mysql表
    val dspBudgetHourExpendTable = "dsp_budget_hour_expend"; //1.2.2 按小时统计广告计划消耗数据
    val dspBudgetHourExpendCr = "dsp_budget_hour_expend_cr"  // 1.2.3 按小时统计广告创意消耗数据
    val dspChannelQuality = "dsp_channel_quality" //1.2.4 按小时统计下游流量数据
    val dspConversionInfo = "dsp_conversion_info"
    val dspDeviceBidCnt = "dsp_device_bid_cnt" // 1.3.1 按天汇总设备转化数据
    val dspBundleBidCnt = "dsp_bundle_bid_cnt" // 1.3.2

    /**
     * 将表预处理,得到一个视图
     * @param spark
     * @param etlDate
     * @param etlHour
     *                先过滤广告主id/ 计划id为空的
     *                然后去重,选时间戳小的一条
     */
    def preprocessTable(spark: SparkSession, etlDate: String, etlHour: String): Unit={
        //日志类型+追踪ID+竞价请求ID+广告位ID 取时间戳小的
        spark.sql(
            s"""
               |select
               |   logtype          ,
               |   time             ,
               |   trace_id         ,
               |   request_id       ,
               |   adsolt_id        ,
               |   adsolt_type      ,
               |   floor_price      ,
               |   currency_type    ,
               |   device_id        ,
               |   admaster_id      ,
               |   plan_id          ,
               |   creative_id      ,
               |   plan_price       ,
               |   real_price       ,
               |   bid_resp_id      ,
               |   bid_resp_code    ,
               |   win_price        ,
               |   price_rate       ,
               |   res_code         ,
               |   clk_id           ,
               |   downstream_id ,
               |   conversion_task_id,
               |    conversion_unit_price,
               |    stuff_id,
               |     stuff_shape,
               |    adsolt_shape,
               |    pkg_name,
               |    country_code,
               |    os
               |from
               |(
               |    select
               |        *,
               |        row_number() over(partition by logtype,trace_id,request_id,adsolt_id order by `time` asc) as r_n -- 取重复的最早出现的一次
               |    from
               |        ods.ods_dsp_info
               |        where etl_date='${etlDate}' and etl_hour='${etlHour}'
               |) t
               |where r_n =1
               |
               |""".stripMargin)
            .cache()
            .createOrReplaceTempView("preprocess_table1")


        // 按天去重
        spark.sql(
            s"""
               |select
               |   logtype          ,
               |   time             ,
               |   trace_id         ,
               |   request_id       ,
               |   adsolt_id        ,
               |   adsolt_type      ,
               |   floor_price      ,
               |   currency_type    ,
               |   device_id        ,
               |   admaster_id      ,
               |   plan_id          ,
               |   creative_id      ,
               |   plan_price       ,
               |   real_price       ,
               |   bid_resp_id      ,
               |   bid_resp_code    ,
               |   win_price        ,
               |   price_rate       ,
               |   res_code         ,
               |   clk_id           ,
               |   downstream_id    ,
               |    conversion_task_id,
               |    conversion_unit_price,
               |    stuff_id,
               |    stuff_shape,
               |    adsolt_shape,
               |    pkg_name,
               |    country_code
               |from
               |(
               |    select
               |        *,
               |        row_number() over(partition by logtype,trace_id,request_id,adsolt_id order by `time` asc) as r_n -- 取重复的最早出现的一次
               |    from
               |        ods.ods_dsp_info
               |        where etl_date='${etlDate}'
               |) t
               |where r_n =1
               |
               |""".stripMargin)
            .persist(StorageLevel.MEMORY_AND_DISK)
            .createOrReplaceTempView("preprocess_table_day")


    }
    /**
     * 1.2.2.按小时统计广告计划消耗数据
     * 需按 "追踪ID+竞价请求ID+广告位ID" 去重
     * http://wiki.vlion.cn/pages/viewpage.action?pageId=36439599
     */
    def planSummary(spark: SparkSession, etlDate: String, etlHour: String): Unit = {
        val etlTime = etlDate + " " + etlHour + ":00:00"
        val summaryDF = spark.sql(
            s"""
               |select
               |    '${etlDate}' as time,
               |    '${etlTime}' as hour,
               |    admaster_id,
               |    plan_id,
               |    max(if(logtype='11' and r_n=1,plan_price,0)) as plan_price, --计划出价,取最新的
               |    max(if(logtype='11' and r_n=1,real_price,0)) as real_price, --实际出价
               |    count(if(logtype='11',1,null)) as plan_bid_count, -- 计划出价次数
               |    count(if(logtype='12',1,null)) as inner_win_count, -- 赢得内部竞价次数
               |    count(if(logtype='13',1,null)) as outer_bid_succ_count, -- 外部竞价成功次数
               |    count(if(logtype='16',1,null)) as outer_bid_fail_count,  -- 外部竞价失败次数
               |    count(if(logtype='14',1,null)) as imp_count,  -- 曝光次数
               |    sum(if(logtype='14',win_price,0)) as real_cost,  -- 实际消耗
               |    count(if(logtype='15',logtype,null)) as clk_count,  -- 点击次数
               |    count(if(logtype='17',logtype,null)) as trans_count,  -- 转化数
               |    '0' as process_status  -- 状态 0: 处理中  1: 成功
               |from (
               |    select
               |        *,
               |        row_number() over(partition by logtype,admaster_id,plan_id order by `time` desc) as r_n -- 每种日志分区,取最大的
               |    from
               |    preprocess_table1
               |    where logtype !='10' and admaster_id is not null and admaster_id != '' -- 广告主id不为空
               |         and plan_id is not null and plan_id != ''  -- 计划id不为空
               |) t
               |group by
               |    admaster_id,plan_id
               |""".stripMargin)

        summaryDF.coalesce(5).write.mode("append")
            .format("jdbc")
            .option("url",url)
            .option("driver",driver)
            .option("user",user)
            .option("password",password)
            .option("dbtable",dspBudgetHourExpendTable)
            .save()
    }

    /**
     * 1.2.3按小时统计广告创意消耗数据
     * 需按 "追踪ID+竞价请求ID+广告位ID" 去重
     * @param spark
     * @param etlDate
     * @param etlHour
     */
    def creativeSummary(spark:SparkSession, etlDate: String, etlHour: String): Unit ={
        val etlTime = etlDate + " " + etlHour + ":00:00"
        val creativeDF = spark.sql(
            s"""
               |select
               |    '${etlDate}' as time,
               |    '${etlTime}' as hour,
               |    admaster_id,
               |    plan_id,
               |    creative_id,
               |    adsolt_type as creative_type,
               |    max(if(logtype='11' and r_n=1,plan_price,0)) as plan_price, --计划出价,取最新的
               |    -- max(if(logtype='11' and r_n=1,real_price,0)) as real_price, --实际出价
               |    count(if(logtype='11',1,null)) as bid_count, -- 计划出价次数
               |    count(if(logtype='12',1,null)) as inner_win_count, -- 赢得内部竞价次数
               |    count(if(logtype='12',1,null)) / count(if(logtype='11',1,null)) as inner_bid_succ_rate, --内部竞价成功率
               |    count(if(logtype='13',1,null)) as outer_bid_succ_count, -- 外部竞价成功次数
               |    count(if(logtype='13',1,null)) / count(if(logtype='12',1,null)) as outer_bid_succ_rate, --外部竞价成功率
               |    count(if(logtype='16',1,null)) as outer_bid_fail_count,  -- 外部竞价失败次数
               |    count(if(logtype='14',1,null)) as imp_count,  -- 曝光次数
               |    count(if(logtype='14',1,null)) / count(if(logtype='13',1,null)) as imp_rate, -- 曝光率
               |    sum(if(logtype='14',win_price,null)) / count(if(logtype='14',1,null)) as avg_win_price, -- 平均结算价
               |    -- sum(if(logtype='14',win_price,0)) as real_cost,  -- 实际消耗
               |    count(if(logtype='15',logtype,null)) as clk_count,  -- 点击次数
               |    count(if(logtype='15',logtype,null)) / count(if(logtype='14',1,null)) as clk_rate, -- 点击率
               |    count(if(logtype='17',logtype,null)) as trans_count,  -- 转化数
               |    count(if(logtype='17',logtype,null)) / count(if(logtype='15',logtype,null)) as trans_rate, -- 转化率
               |    sum(if(logtype='14',win_price,null)) /1000 as total_cost, -- 总消耗
               |    (sum(if(logtype='14',win_price,null)) / 1000 ) / count(if(logtype='17',1,null)) as trans_cost, -- 转化成本
               |    sum(if(logtype='17',conversion_unit_price,null)) as total_earning, -- 总收益
               |    sum(if(logtype='17',conversion_unit_price,null)) - sum(if(logtype='14',win_price,null)) /1000 as profit, -- 利润
               |    '0' as process_status  -- 状态 0: 处理中  1: 成功
               |from (
               |    select
               |        *,
               |        row_number() over(partition by logtype,admaster_id,plan_id,creative_id,adsolt_type order by `time` desc) as r_n -- 每种日志分区,取最大的
               |    from
               |        preprocess_table1
               |    where logtype !='10'
               |    ) t
               |group by
               |    admaster_id,plan_id,creative_id,adsolt_type
               |""".stripMargin)

        creativeDF
            .coalesce(5)
            .write.mode("append")
            .format("jdbc")
            .option("url",url)
            .option("driver",driver)
            .option("user",user)
            .option("password",password)
            .option("dbtable",dspBudgetHourExpendCr)
            .save()

    }


    /**
     * 1.2.4 按小时统计下游流量数据
     * 需按 "追踪ID+竞价请求ID" 去重
     * @param spark
     * @param etlDate
     * @param etlHour
     */
    def downstreamSummary(spark:SparkSession, etlDate: String, etlHour: String): Unit ={
        val etlTime = etlDate + " " + etlHour + ":00:00"
        val downstreamDF =spark.sql(
            s"""
               |select
               |    '${etlDate}' as time,
               |    '${etlTime}' as hour,
               |    downstream_id,
               |    adsolt_type, -- 广告位类型
               |    count(if(logtype = '10',1,null)) as  req_count, -- 请求总数
               |    int(count(if(logtype = '10',1,null))/3600) as qps,
               |    sum(if(logtype='10',floor_price,null)) / count(if(logtype = '10',1,null)) as avg_floor_price, -- 平均底价
               |    -- avg(if(logtype ='14', win_price,null)) as avg_win_price, -- 平均结算价
               |    count(if(logtype = '10' and res_code ='400',1,null)) as abnormal_req_count, -- 异常请求数
               |    count(if(logtype='12',1,null)) as fill_resp_count, -- 填充响应数
               |    avg(if(logtype='12', plan_price ,null)) avg_bid_price ,-- 平均出价
               |    count(if(logtype='12',1,null)) / count(if(logtype = '10',1,null)) as fill_rate, -- 填充率
               |    count(if(logtype='11',1,null)) as plan_bid_count, -- 计划出价总次数
               |    -- count(if(logtype='12',1,null)) as inner_win_count, -- 赢得内部竞价次数
               |    count(if(logtype='13',1,null)) as outer_bid_succ_count, -- 外部竞价成功总次数
               |    count(if(logtype='13',1,null)) / count(if(logtype='12',1,null)) as bid_win_rate, -- 竞价成功率
               |    count(if(logtype='16',1,null)) as outer_bid_fail_count, -- 外部竞价失败总次数
               |    count(if(logtype='14',1,null)) as imp_count, -- 曝光总次数
               |    sum(if(logtype='14',win_price,null)) / count(if(logtype='14',1,null)) as avg_win_price, -- 平均结算价
               |    count(if(logtype='14',1,null)) / count(if(logtype='13',1,null)) as imp_rate, -- 曝光率
               |    -- sum(if(logtype='14',win_price,0)) as real_cost,  -- 实际消耗
               |    count(if(logtype='15',logtype,null)) as clk_count,  -- 点击总次数
               |    count(if(logtype='15',logtype,null)) / count(if(logtype='14',logtype,null)) as clk_rate, -- 点击率
               |    -- count(if(logtype='17',logtype,null)) as trans_count,  -- 转化数
               |    sum(if(logtype='14',win_price,null)) / 1000 as total_cost, -- 总消耗
               |    (sum(if(logtype='14',win_price,null)) / 1000 ) / count(if(logtype='15',logtype,null)) as clk_cost, -- 点击成本
               |    '0' as process_status
               |from
               |   preprocess_table1 t
               |   group by
               |    downstream_id, -- 下游id
               |    adsolt_type -- 广告位id
               |""".stripMargin)

        downstreamDF
            .coalesce(5)
            .write.mode("append")
            .format("jdbc")
            .option("url",url)
            .option("driver",driver)
            .option("user",user)
            .option("password",password)
            .option("dbtable",dspChannelQuality)
            .save()
    }


    /**
     * 1.2.5 按天统计转化明细数据
     * 需按 "日志类型+追踪ID+竞价请求ID+广告位ID" 去重
     * @param spark
     * @param etlDate
     */
    def conversionDaySummary(spark:SparkSession,etlDate:String): Unit ={
        val df = spark.sql(
            s"""
               |select
               |    '${etlDate}' as date,
               |    plan_id,
               |    creative_id,
               |    stuff_id,
               |    conversion_task_id as conv_id,
               |    count(if(logtype='11',1,null)) as offer_req,
               |    count(if(logtype='12',1,null)) as fill_req, -- 填充响应次数
               |    avg(if(logtype='12',plan_price,null)) as bid_price_avg, -- 平均出价
               |    count(if(logtype='13',1,null)) as ssp_win, -- 外部竞价成功数
               |    count(if(logtype='13',1,null)) / count(if(logtype='12',1,null)) as win_rate,
               |    count(if(logtype='14',1,null)) as imp, -- 曝光总数
               |    count(if(logtype='14',1,null)) / count(if(logtype='13',1,null)) as imp_rate,
               |    avg(if(logtype='14',win_price,null)) as clear_price_avg, -- 平均结算价
               |    count(if(logtype='15',1,null)) as clk,-- 点击数
               |    count(if(logtype='15',1,null)) / count(if(logtype='14',1,null)) as ctr, --点击率
               |    count(if(logtype='17',1,null)) as conversion,-- 转化数
               |    count(if(logtype='17',1,null)) / count(if(logtype='15',1,null)) as cvr, -- 转化率
               |    sum(if(logtype='14',win_price,null)) /1000 as cost, -- 总花费
               |    sum(if(logtype='17',conversion_unit_price,null)) as revenue, -- 总收入
               |    sum(if(logtype='17',conversion_unit_price,null)) - sum(if(logtype='14',win_price,null)) /1000 as margin-- 利润
               |from
               |    preprocess_table_day
               |group by
               |    plan_id,
               |    creative_id,
               |    stuff_id,
               |    conversion_task_id  -- 任务id
               |
               |""".stripMargin)

//        val allColsSeq = df.schema.map(sf => sf.name)
//        val broadcast = spark.sparkContext.broadcast(allColsSeq)
        // 更新dsp_conversion_info这张表
        insertUpdateMysql(spark,df,5, dspConversionInfo)
    }

    /**
     * 1.3.1 按天汇总设备转化数据
     * @param spark
     * @param etlDate
     */
    def deviceConversionDay(spark:SparkSession,etlDate:String)={
        spark.sql(
            s"""
               |select
               |    '${etlDate}' as date,
               |    device_id,
               |    downstream_id  as channel_id,
               |    plan_id,
               |    conversion_task_id as conv_task,
               |    avg(if(logtype='11',floor_price,null)) as floor_avg,
               |    count(if(logtype='12',1,null)) as fill_req,
               |    avg(if(logtype='11',plan_price,null)) as bid_price_avg,
               |    count(if(logtype='13',1,null)) as ssp_win,
               |    count(if(logtype='14',1,null)) as imp,
               |    avg(if(logtype='14',win_price,null)) as clear_price_avg,
               |    count(if(logtype='15',1,null)) as clk,
               |    count(if(logtype='15',1,null)) / count(if(logtype='14',1,null)) as ctr,
               |    count(if(logtype='17',1,null)) as conversion,
               |    count(if(logtype='17',1,null)) / count(if(logtype='15',1,null)) as cvr
               |from
               | preprocess_table_day
               |group by
               |    device_id,
               |    downstream_id,
               |    plan_id,
               |    conversion_task_id
               |""".stripMargin).persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("device_conversion_view")

//        insertUpdateMysql(spark,dfConversionDay,4, dspDeviceBidCnt)
        val deviceConversionDF = spark.sql(
            s"""
               |(
               |select
               |    *,1 as summary_type
               |from
               |    device_conversion_view
               |where
               |    conversion > 0
               |)
               |
               |union all
               |
               |(
               |select
               |    *,2 as summary_type
               |from
               |    device_conversion_view
               |where
               |    imp >= 100 and ctr <= 0.0005
               |    order by imp desc,ctr asc limit 100  -- 还有排序,用同一个表查询会比较复杂
               |)
               |
               |union all
               |
               |(
               |select
               |    *,3 as summary_type
               |from
               |    device_conversion_view
               |where
               |    ssp_win >= 100 and (imp / ssp_win) * 100 < 0.1
               |order by ssp_win desc,(imp / ssp_win) * 100 asc limit 100  -- 还有排序,用同一个表查询会比较复杂
               |)
               |""".stripMargin)


        insertUpdateMysql(spark,deviceConversionDF,5, dspDeviceBidCnt)


    }

    /**
     * 1.3.2 按天汇总包名转化数据
     * 需按 "日志类型+追踪ID+竞价请求ID+广告位ID" 去重
     *
     * 汇总结果：
     *
     * 优质包名指标：
     * Imp > 100 and CTR > 2%，Top 100
     * IVT 包名指标：
     * Imp > 1000 and CTR < 0.05%，Top 100
     *
     * @param spark
     * @param etlDate
     */
    def bundleConversionDay(spark:SparkSession, etlDate:String): Unit ={
        spark.sql(
            s"""
               |select
               |    '${etlDate}' as date,
               |    pkg_name as bundle,
               |    downstream_id  as channel_id,
               |    plan_id,
               |    conversion_task_id as conv_task,
               |    avg(if(logtype='11',floor_price,null)) as floor_avg,
               |    count(if(logtype='12',1,null)) as fill_req,
               |    avg(if(logtype='11',plan_price,null)) as bid_price_avg,
               |    count(if(logtype='13',1,null)) as ssp_win,
               |    count(if(logtype='14',1,null)) as imp,
               |    avg(if(logtype='14',win_price,null)) as clear_price_avg,
               |    count(if(logtype='15',1,null)) as clk,
               |    count(if(logtype='15',1,null)) / count(if(logtype='14',1,null)) as ctr,
               |    count(if(logtype='17',1,null)) as conversion,
               |    count(if(logtype='17',1,null)) / count(if(logtype='15',1,null)) as cvr
               |from
               | preprocess_table_day
               |group by
               |    pkg_name,
               |    downstream_id,
               |    plan_id,
               |    conversion_task_id
               |""".stripMargin).persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView("device_info_view")

        val dspBundleBidCntDF = spark.sql(
            s"""
               |(
               |select
               |    *,1 as summary_type
               |from
               |    device_info_view
               |where imp >= 100 and ctr >= 0.02
               |order by imp desc ,ctr desc
               |limit 100
               |)
               |
               |union all
               |(
               |select
               |    *,2 as summary_type
               |from
               |    device_info_view
               |where imp >= 100 and ctr <= 0.0005
               |order by imp desc, ctr asc
               |limit 100
               |)
               |
               |union all
               |(
               |select
               |    *,3 as summary_type
               |from
               |    device_info_view
               |where ssp_win >= 100 and (imp / ssp_win) * 100 < 0.1
               |order by ssp_win desc,(imp / ssp_win) * 100 asc limit 100
               |)
               |
               |""".stripMargin)
        insertUpdateMysql(spark,dspBundleBidCntDF,5, dspBundleBidCnt)

    }

    /**
     * 1.3.3 按小时统计底价范围数据
     * 表名：dsp_bidfloor_cnt
     *
     * 需按 "日志类型+追踪ID+竞价请求ID+广告位ID" 去重
     *
     * 汇总"包名+广告位尺寸"维度总请求数大于1000的记录
     */
    def dspBidFloorCntHour(spark:SparkSession, etlDate:String,etlHour:String): Unit ={
        val rangeList = Range(15,51,5)
        val rangeList1 = Range(10,46,5)
        // 左开右闭

        val partSql = rangeList1.zip(rangeList).map { case (leftIndex, rightIndex) =>
            s"""
               |count(if(logtype='10' and floor_price > $leftIndex and floor_price <= $rightIndex,1,null)) as req_less_$rightIndex,
               |    count(if(logtype='12' and floor_price > $leftIndex and floor_price <= $rightIndex ,1,null)) as fill_less_$rightIndex,
               |    count(if(logtype='13' and floor_price > $leftIndex and floor_price <= $rightIndex ,1,null)) as win_less_$rightIndex
               |""".stripMargin
        }.mkString(",")





        val frame = spark.sql(
            s"""
               |select
               |    data_hour,
               |    bundle,
               |    ad_size,
               |    req_less_10,
               |    fill_less_10,
               |    win_less_10,
               |    ${rangeList.map(index => s"req_less_${index}, fill_less_$index, win_less_$index").mkString(",")},
               |    req_great_50,
               |    fill_great_50,
               |    win_great_50
               |from
               |(
               |    select
               |        '${etlDate} $etlHour:00:00' as data_hour,
               |        pkg_name as bundle,
               |        adsolt_shape as ad_size,
               |        count(if(logtype='10' and floor_price <= 10,1,null)) as req_less_10,
               |        count(if(logtype='12' and floor_price <=10 ,1,null)) as fill_less_10,
               |        count(if(logtype='13' and floor_price <=10 ,1,null)) as win_less_10,
               |       ${partSql},
               |       count(if(logtype='10' and floor_price > 50,1,null)) as req_great_50,
               |        count(if(logtype='12' and floor_price > 50 ,1,null)) as fill_great_50,
               |        count(if(logtype='13' and floor_price > 50 ,1,null)) as win_great_50,
               |        sum(if(logtype='10',1,0)) as log10_cnt
               |    from
               |    preprocess_table1
               |    group by
               |        pkg_name,adsolt_shape
               |) t
               |where log10_cnt > 1000
               |""".stripMargin)

        insertUpdateMysql(spark,frame,2,"dsp_bidfloor_cnt")


    }

    /**
     * 1.4 广告全量数据预聚合
     * 按 "日志类型+追踪ID+竞价请求ID+广告位ID" 去重
     *
     * 预聚合数据写入大数据支持各维度聚合实时查询
     *
     * @param spark
     * @param etlDate
     * @param etlHour
     */
    def adDimensionPreSummary(spark:SparkSession,etlDate:String,etlHour:String)={
        spark.sql(
            s"""
               |select
               |    '$etlDate' as date,
               |    '$etlHour' as hour,
               |    downstream_id,
               |    country_code,
               |    adsolt_type,
               |    adsolt_shape,
               |    os,
               |    pkg_name,
               |    admaster_id,
               |    plan_id,
               |    creative_id,
               |    stuff_id,
               |    stuff_shape,
               |    conversion_task_id,
               |    count(if(logtype='10',1,null)) as total_req,
               |    sum(if(logtype='10',floor_price,null)) as bid_floor_sum,
               |    count(if(logtype='11',1,null)) as offer_bid,
               |    sum(if(logtype='11',plan_price,null)) as offer_price,
               |    count(if(logtype='12',1,null)) as fill_req,
               |    sum(if(logtype='12',plan_price,null)) as bid_price,
               |    count(if(logtype='13',1,null)) as ssp_win,
               |    count(if(logtype='16',1,null)) as ssp_loss,
               |    count(if(logtype='14',1,null)) as imp,
               |    sum(if(logtype='14',win_price,null)) as clear_price,
               |    count(if(logtype='15',1,null)) as clk,
               |    count(if(logtype='17',1,null)) as conversion,
               |    sum(if(logtype='14',win_price,null)) as cost,
               |    sum(if(logtype='17',conversion_unit_price,null)) as revenue
               |from
               |    preprocess_table1
               |group by
               |    downstream_id,
               |    country_code,
               |    adsolt_type,
               |    adsolt_shape,
               |    os,
               |    pkg_name,
               |    admaster_id,
               |    plan_id,
               |    creative_id,
               |    stuff_id,
               |    stuff_shape,
               |    conversion_task_id
               |
               |""".stripMargin)
            .coalesce(1)
            .write
            .mode("overwrite")
            .partitionBy("date","hour")
            .saveAsTable("dm.dsp_detail_data_count")

    }

    // 自定义导入mysql
    private def insertUpdateMysql(spark:SparkSession, df:DataFrame,keyNum:Int,targetTable:String) = {
        val allColsSeq =df.schema.map(sf => sf.name).toList
        val broadcast = spark.sparkContext.broadcast(allColsSeq)

        df.rdd.coalesce(4).foreachPartition(iter => {
            val list = iter.toList
//            println("list.size: "+list.size)

            val allColsSeq =broadcast.value
            val allCols = allColsSeq.mkString(",")
            val updateCols = allColsSeq.slice(keyNum,allColsSeq.size).map(col => s"${col}=values(${col})").mkString(",")

            Class.forName(MySQL.driver)
            val conn = DriverManager.getConnection(MySQL.url, MySQL.user, MySQL.password)

            val `num?` = allColsSeq.indices.map(_ => "?" ).mkString(",")

            val pstmt = conn.prepareStatement(s"insert into $targetTable (${allCols}) values(${`num?`}) ON DUPLICATE key update ${updateCols}")
//            println(s"insert into $targetTable (${allCols}) values(${`num?`}) ON DUPLICATE key update ${updateCols}")
            val colTuple = allColsSeq.zipWithIndex.map(t  => (t._1,t._2+1))
//            println("colTuple: "+colTuple)

            list.map(row => {
//                println("=" * 10)
//                println(row)
//                println("=" * 10)

                colTuple.foreach { case (colName, pstmtIndex) =>
                    // println(pstmtIndex,row.getAs[String](colName))
                    // 插入每条数据
                    pstmt.setObject(pstmtIndex,row.getAs[String](colName))
                }
                pstmt.execute()
            })
            pstmt.close()
            conn.close()
        })
    }
}
