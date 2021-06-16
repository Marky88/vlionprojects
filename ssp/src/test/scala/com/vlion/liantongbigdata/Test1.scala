package com.vlion.liantongbigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * @description:
 * @author: malichun
 * @time: 2021/4/21/0021 15:08
 *
 */
object Test1 {
    def main(args: Array[String]): Unit = {
        //构建 SparkConf 对象
        val sparkConf: SparkConf = new SparkConf().setAppName("DataFrameKudu").setMaster("local[2]")
        //构建 SparkSession 对象
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._

        //获取 SparkContext 对象
        val sc: SparkContext = spark.sparkContext

        val inputDir = "/files/ftp_cluster_222/opdw4_234"
        val etlDate = "20210417"

        //        val rdd = sc.textFile(s"${inputDir}/*${etlDate}*")

        val schema = StructType(
            StructField("name", StringType)
                :: StructField("age", IntegerType)

                :: Nil
        )

        val rdd = sc.textFile("/files/ftp_cluster_222/opdw4_234/log")
        rdd.mapPartitions { iter =>
            iter.map(line => {
                line.split("\\|")
            })
        }


    }



    /**
     * 需求1:计算每个URL/包名的用户占比；
     *
     * @return
     */
    def summaryCount(spark: SparkSession, sc: SparkContext) = {
        import spark.implicits._

        // 人工url
        val urlRDD = sc.textFile("/files/ftp_cluster_222/opdw4_234/process/20210423/package_name.txt")
            .map(line => line.trim.replaceAll("\t", ""))
            .distinct()
        val urlSet = urlRDD.collect().toSet
        val urlBroadCast = sc.broadcast(urlSet)


        // 上网日志
        val rdd = sc.textFile("/files/ftp_cluster_222/opdw4_234/log/shrs_*_20210417_*.gz")
//        val rdd = sc.textFile("/files/ftp_cluster_222/opdw4_234/log")
        val containsRDD = rdd.mapPartitions { iter =>
            val urlSet = urlBroadCast.value
            iter.map(_.split("\\|"))
                .collect { case arr if arr.length == 26 && urlSet.contains(arr(25)) =>
                    (arr(3), arr(25)) // imei,url
                }
        }

        containsRDD.toDF("imei", "url_col").createOrReplaceTempView("contain_table")

        spark.sql(
            s"""
               |select
               |    count(imei) as count_imei,
               |    count(distinct imei) as count_distinct_imei
               |from
               |    contain_table
               |group by url_col
               |
               |""".stripMargin).show(1000,false)

    }

    //看是否真的不存在
    def summaryCount2(spark: SparkSession, sc: SparkContext) = {
        import spark.implicits._

        // 人工url
        val urlRDD = sc.textFile("/files/ftp_cluster_222/opdw4_234/process/20210423/package_name.txt")
            .map(line => line.trim.replaceAll("\t", ""))
            .distinct()


        // 上网日志
        val rdd = sc.textFile("/files/ftp_cluster_222/opdw4_234/log/shrs_*_20210417_*.gz")
        rdd.map(_.split("\\|")).filter(_.length == 26).map(arr => s"${arr(3)}|${arr(25)}")
            .saveAsTextFile("/files/ftp_cluster_222/opdw4_234/out/shrs_20210417_out")




//
        val rdd_log = sc.textFile("/files/ftp_cluster_222/opdw4_234/out/shrs_20210417_out")
        val rdd_package = sc.textFile("/files/ftp_cluster_222/opdw4_234/process/20210423/package_name.txt")

        val packageSet = rdd_package.collect().toSet




        val rddLog2 = rdd_log.map(line => {
            val arr = line.split("\\|")
            (arr(1),arr(0))
        })

        val rdd_package2 = rdd_package.map((_,1))


        val value = rddLog2.join(rdd_package2)

        value.count // 输出count


        value.map(t => (t._1,t._2._1)) //(url,imei)
            .toDF("url","imei")
            .createOrReplaceTempView("")


    }

}
