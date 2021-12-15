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
             //   .master("local[*]")
            .appName("AdxSaasDspBI")
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
        OfflineStatistics.preprocessTable(spark,etlDate,etlHour)    //ods.ods_dsp_info表按天/小时预处理
    //    OfflineStatistics.planSummary(spark,etlDate,etlHour)
    //    OfflineStatistics.creativeSummary(spark,etlDate,etlHour)   //已修改写入CK 脚本需要修改 process_status字段需处理

     //   OfflineStatistics.downstreamSummary(spark,etlDate,etlHour)
//        OfflineStatistics.preprocessTable(spark,etlDate,etlHour)


        OfflineStatistics.adDimensionPreSummary(spark,etlDate,etlHour) // 1.4广告全量数据预聚合

        OfflineStatistics.dspWinrateCntHour(spark,etlDate,etlHour)    //1.3.4  按小时统计 WinRate 数据

    }
}
