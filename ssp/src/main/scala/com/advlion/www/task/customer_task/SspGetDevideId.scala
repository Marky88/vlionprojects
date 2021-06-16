package com.advlion.www.task.customer_task

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/5/7/0007 14:06
 *
 *        按天生成imei,idfa,imei文件,去重,全部是md5类型的
 *
 */
object SspGetDevideId {
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
        val sc = spark.sparkContext
        import spark.implicits._

        val etlDate = args(0)
        val inputBasePath = args(1)
        val outBasePath = args(2)


        val inputPath = inputBasePath + "/" + etlDate
        val outPath = outBasePath + "/" + etlDate


        val rdd = sc.textFile(inputPath)

        val rdd1 = rdd.map { line =>
            line.split("\t")
        }.filter(_.length > 37).map(arr => {
            Array(("imei", arr(7)), ("idfa", arr(10)), ("oaid", arr(36)))
        }).flatMap(x => x)

        rdd1.toDF("type", "id").createOrReplaceTempView("table1")

        //        spark.sqlContext.setConf("spark.sql.shuffle.partitions","300")

        spark.sql(
            s"""
               |select
               |    distinct type,if(length(id)=32,id,md5(id)) as id
               |from
               |    table1
               |where
               |    (type = 'imei'  and id is not null and id != '' and id != '000000000000000')
               |    or
               |    (type = 'idfa' and id is not null and id != '')
               |    or
               |    (type = 'oaid' and id is not null and id != '' and id != '00000000-0000-0000-0000-000000000000')
               |
               |""".stripMargin)
            .rdd
            .map(r => {
                r.getAs[String]("type") + "," + r.getAs[String]("id")
            })
            .saveAsTextFile(outPath)


    }


}
