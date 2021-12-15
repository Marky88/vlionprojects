package com.vlion.task


import com.vlion.analysis.Analysis
import com.vlion.utils.Bundles
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author:
 * @time: 2021/12/7/0007 9:47
 *
 */
object AlgorithmDSP {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .appName("algorithm_dsp")
          //  .master("local[2]")
            .config("hive.metastore.uris", "trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
            .config("spark.debug.maxToStringFields", "100")
            .enableHiveSupport()
            .getOrCreate()

        val etl_date = args(0)

        val bundleArr:Array[String] = Bundles.bundles


        Analysis.summaryDay(spark,etl_date,bundleArr)


    }

}
