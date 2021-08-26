package com.advlion.www.task

import com.advlion.www.analysis.Summary
import com.advlion.www.sync.ImportData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Zebra{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
//      .master("local[*]")
      .appName("Zebra")
      .config("hive.metastore.uris", "thrift://www.bigdata02.com:9083")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
      .config("spark.debug.maxToStringFields", "100")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
      try {
        ImportData.importMySQL(spark: SparkSession)
        Summary.readSaAccess(spark, args)

        Summary.readAccess(spark, args)
      }catch{
          case e: Exception => {
              e.printStackTrace()
              sys.exit(4)
          }
      }finally {
          spark.stop()
      }
  }
}
