package com.vlion.adx_saas_dsp.task

import com.vlion.adx_saas_dsp.analysis.OfflineStatistics
import com.vlion.adx_saas_dsp.load.LoadMySql
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/6/9/0009 15:52
 *  需求文档地址: http://wiki.vlion.cn/pages/viewpage.action?pageId=36439599
 */
object AdxSaasDsp {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession
            .builder()
            //     .master("local[*]")
            .appName("AdxSaasDsp")
            .config("hive.metastore.uris","trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir","/user/hive/warehouse")
            .config("spark.debug.maxToStringFields","100")
            .enableHiveSupport()
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        val etlDate = args(0)
        val etlHour = args(1)
        // 1.2.2 按小时统计广告计划消耗数据
//        LoadMySql.readMysql(spark) // 加载mysql的数据
        OfflineStatistics.preprocessTable(spark,etlDate,etlHour)
        OfflineStatistics.planSummary(spark,etlDate,etlHour)
        OfflineStatistics.creativeSummary(spark,etlDate,etlHour)
        OfflineStatistics.downstreamSummary(spark,etlDate,etlHour)
        OfflineStatistics.preprocessTable(spark,etlDate,etlHour)

        OfflineStatistics.conversionDaySummary(spark,etlDate)
//        OfflineStatistics.deviceConversionDay(spark,etlDate)
    }
}
