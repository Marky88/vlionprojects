package com.vlion

import com.vlion.statistics.Statistics
import com.vlion.udfs.{MaxCountColUDAF, ParseBrandUDF, TimeUDAF}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @description:
 * @author: malichun
 * @time: 2021/7/22/0022 14:18
 *
 */
object Task {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession
            .builder()
            //     .master("local[*]")
            .config("hive.metastore.uris","trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir","/user/hive/warehouse")
            .config("spark.debug.maxToStringFields","100")
            .enableHiveSupport()
            .getOrCreate()

        Statistics.summaryDay(spark,args(0))
        Statistics.summaryCalculateDay(spark,args(0),3)
        Statistics.summaryCalculateDay(spark,args(0),7)
        Statistics.summaryCalculateDay(spark,args(0),14)
        spark.stop


    }

    def demo(spark:SparkSession): Unit ={

        spark.udf.register("parse_brand",new ParseBrandUDF().parseBrand _ )

        spark.sql(
            """
              |
              |select
              |c
              |from
              |(
              |select
              |  'a,b,c,d,a,a,a,a,b' as cols
              | ) t
              |  lateral view explode(split(cols,',')) m as c
              |
              |
              |
              |""".stripMargin).createOrReplaceTempView("test")


        spark.udf.register("max_count_col",new MaxCountColUDAF)

        spark.sql(
            """
              |select
              |max_count_col(c)
              |from
              |test
              |""".stripMargin).show


        spark.udf.register("time_process",new TimeUDAF("2021-07-23","12")) //1626926400

        spark.sql(
            """
              |select
              |c
              |from
              |(
              |select
              |  '1626926499,1626926401,1626926403,1626926800,1626929999' as cols
              | ) t
              |  lateral view explode(split(cols,',')) m as c
              |
              |""".stripMargin).createOrReplaceTempView("test1")

        spark.sql(
            """
              |select
              |time_process(c)
              |from
              | test1
              |""".stripMargin).show(false)


        spark.sql("").rdd.map((row: Row) => {

        })
    }

}
