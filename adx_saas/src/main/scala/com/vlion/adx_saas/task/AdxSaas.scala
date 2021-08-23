package com.vlion.adx_saas.task

import com.vlion.adx_saas.analysis.OfflineStatistics
import com.vlion.adx_saas.load.{LoadHive, LoadMySql}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/7/8/0008 17:10
 *
 */
object AdxSaas {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        implicit val spark: SparkSession = SparkSession
            .builder()
            //     .master("local[*]")
            .appName("AdxSaas")
            .config("hive.metastore.uris","trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir","/user/hive/warehouse")
            .config("spark.debug.maxToStringFields","100")
            .enableHiveSupport()
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        val etlDate = args(0)
        val etlHour = args(1)


        OfflineStatistics.updateMysqlPkg(spark,etlDate,etlHour) // 先更新维护的mysql表的数据
        LoadMySql.readMysql // 加载mysql的数据
        LoadHive.loadHive(spark,etlDate,etlHour)
        OfflineStatistics.hourSummary(spark,etlDate,etlHour)

        spark.sparkContext.stop()

    }

}
