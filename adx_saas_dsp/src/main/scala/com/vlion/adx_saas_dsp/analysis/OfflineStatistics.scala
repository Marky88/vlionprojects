package com.vlion.adx_saas_dsp.analysis

import com.vlion.adx_saas_dsp.jdbc.MySQL
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
    val dspBudgetHourExpendTable = "dsp_budget_hour_expend";
    /**
     * 按小时统计消耗数据
     * http://wiki.vlion.cn/pages/viewpage.action?pageId=36439599
     */
    def consumeSummary(spark: SparkSession, etlDate: String, etlHour: String): Unit = {
        val etlTime = etlDate + " " + etlHour + ":00:00"
        val summaryDF = spark.sql(
            s"""
               |select
               |    '${etlDate}' as time,
               |    '${etlTime}' as hour,
               |    admaster_id,
               |    plan_id,
               |    max(if(logtype='12' and r_n=1,plan_price,0)) as plan_price, --计划出价,取最新的
               |    max(if(logtype='12' and r_n=1,real_price,0)) as real_price, --实际出价
               |    count(if(logtype='11',1,null)) as plan_bid_count, -- 计划出价次数
               |    count(if(logtype='12',1,null)) as inner_win_count, -- 赢得内部竞价次数
               |    count(if(logtype='13',1,null)) as outer_bid_succ_count, -- 外部竞价成功次数
               |    count(if(logtype='16',1,null)) as outer_bid_fail_count,  -- 外部竞价失败次数
               |    count(if(logtype='14',1,null)) as imp_count,  -- 曝光次数
               |    sum(if(logtype='14',win_price,0)) as real_cost,  -- 实际消耗
               |    count(if(logtype='15',logtype,null)) as clk_count,  -- 点击次数
               |    '0' as process_status  -- 状态 0: 处理中  1: 成功
               |from
               |    (
               |        select
               |        *,
               |        row_number() over(partition by logtype order by `time` desc) as r_n -- 每种日志分区,取最大的
               |        from ods.ods_dsp_info
               |        where  etl_date='${etlDate}' and etl_hour='${etlHour}' and logtype !='10'
               |    ) t
               |group by
               |    admaster_id,plan_id
               |
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

}
