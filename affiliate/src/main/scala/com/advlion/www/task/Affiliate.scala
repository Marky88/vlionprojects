package com.advlion.www.task

import com.advlion.www.analysis.OfflineStatistics
import com.advlion.www.load.LoadHive
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2020/11/4/0004 19:02
 *
 */
object Affiliate {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession
            .builder()
            //     .master("local[*]")
            .appName("Affiliate")
            .config("hive.metastore.uris","trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir","/user/hive/warehouse")
            .config("spark.debug.maxToStringFields","100")
            .enableHiveSupport()
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        if(args.length != 2){
            println("输入参数错误,程序退出")
            sys.exit(4)
        }
        val etlDate = args(0)
        val etlHour = args(1)
        try {
            LoadHive.readHive(spark, etlDate, etlHour)
            OfflineStatistics.summary(spark, etlDate, etlHour)
        }catch {
            case e:Exception => print("程序错误")
        }finally {
            spark.stop()
            spark.sparkContext.stop()
        }

    }
}
