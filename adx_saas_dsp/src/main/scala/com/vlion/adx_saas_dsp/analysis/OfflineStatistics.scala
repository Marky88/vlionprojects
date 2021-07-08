package com.vlion.adx_saas_dsp.analysis

import com.vlion.adx_saas_dsp.jdbc.MySQL
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

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
               |   downstream_id
               |from
               |(
               |    select
               |        *,
               |        row_number() over(partition by logtype,trace_id,request_id,adsolt_id order by `time` asc) as r_n -- 取重复的最早出现的一次
               |    from
               |        ods.ods_dsp_info
               |        where etl_date='${etlDate}' and etl_hour='${etlHour}'
               |        and admaster_id is not null and admaster_id != '' -- 广告主id不为空
               |         and plan_id is not null and plan_id != ''  -- 计划id不为空
               |) t
               |where r_n =1
               |
               |""".stripMargin)
            .cache()
            .createOrReplaceTempView("preprocess_table1")
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
               |    where logtype !='10'
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
               |    t1.time,
               |    t1.hour,
               |    t1.admaster_id,
               |    t1.plan_id,
               |    t1.creative_id,
               |    t1.creative_type,
               |    t1.plan_price,
               |    t1.bid_count,
               |    t1.inner_win_count,
               |    t1.inner_bid_succ_rate,
               |    t1.outer_bid_succ_count,
               |    t1.outer_bid_succ_rate,
               |    t1.outer_bid_fail_count,
               |    t1.imp_count,
               |    t1.imp_rate,
               |    t1.avg_win_price,
               |    t1.clk_count,
               |    t1.clk_rate,
               |    t1.trans_count,
               |    t1.trans_rate,
               |    t1.total_cost,
               |    t1.trans_cost,
               |    t2.conversion_unit_price * t1.trans_count as total_earning, -- 总收益
               |    t2.conversion_unit_price * t1.trans_count - t1.total_cost as profit, -- 利润
               |    '0' as process_status  -- 状态 0: 处理中  1: 成功
               |from
               |(
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
               |    sum(if(logtype='14',win_price,null)) as total_cost, -- 总消耗
               |    sum(if(logtype='14',win_price,null)) / count(if(logtype='17',logtype,null)) as trans_cost -- 转化成本
               |    -- '0' as process_status  -- 状态 0: 处理中  1: 成功
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
               |) t1
               |    left join
               |dsp_campaign_fake t2
               |    on t1.plan_id = t2.plan_id
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
               |    sum(if(logtype='14',win_price,null)) as total_cost, -- 总消耗
               |    '0' as process_status
               |from
               |   ( -- 先去个重,去时间小的一条
               |    select
               |        *,
               |        row_number() over(partition by logtype,trace_id,request_id,adsolt_id order by `time` asc) as r_n -- 取重复的最早出现的一次
               |    from
               |        ods.ods_dsp_info
               |        where etl_date='${etlDate}' and etl_hour='${etlHour}'
               |  ) t
               |  where r_n = 1
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

}
