package com.advlion.www.task

import com.advlion.www.analysis.OfflineStatistics
import com.advlion.www.load.LoadHive
import com.advlion.www.sync.ImportData
import org.apache.spark.sql.SparkSession

object Mammu {
  def main(args: Array[String]): Unit = {
    require(args.length == 2)

    val spark = SparkSession
      .builder()
      //     .master("local[*]")
      .appName("Mammu_Ssp_BI")
      .config("hive.metastore.uris","thrift://www.bigdata02.com:9083")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.warehouse.dir","/user/hive/warehouse")
      .config("spark.debug.maxToStringFields","100")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ImportData.importMySQL(spark:SparkSession)
    LoadHive.readHive(spark:SparkSession,args: Array[String])
    OfflineStatistics.summary(spark: SparkSession)

    //先暂时不跑,mongodb跑不动
 //   OfflineStatistics.uploadToMongoDB(spark,args(0),args(1))
  }
}
