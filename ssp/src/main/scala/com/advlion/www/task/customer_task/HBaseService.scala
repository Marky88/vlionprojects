package com.advlion.www.task.customer_task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/7/29/0029 9:54
 *
 * id库导入hbase
 */
object HBaseService {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession
            .builder()
            //     .master("local[*]")
            .appName("SspGetDevideId")
            .config("hive.metastore.uris", "trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
            .config("spark.debug.maxToStringFields", "100")
            .enableHiveSupport()
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")




        spark.stop()
        spark.sparkContext.stop()
    }
}
