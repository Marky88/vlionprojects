package com.vlion.testscala

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: malichun
 * @time: 2021/1/19/0019 19:32
 *
 */
object Test {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf())

//        val rdd = sc.textFile("/data/spark/ssp/imei_idfa_oaid/out/*/*")
//
//
//        val rdd2 = rdd.map(line => {
//            val arr = line.split(",")
//            (arr(0),arr(1))
//        }).distinct.cache
//
//
//        rdd2
//            .filter(_._1=="imei")
//            .map(_._2)
//            .saveAsTextFile("/data/spark/ssp/imei_idfa_oaid/out_imei")
//
//
//        rdd2
//            .filter(_._1=="idfa")
//            .map(_._2)
//            .saveAsTextFile("/data/spark/ssp/imei_idfa_oaid/out_idfa")
//
//
//        rdd2
//            .filter(_._1=="oaid")
//            .map(_._2)
//            .saveAsTextFile("/data/spark/ssp/imei_idfa_oaid/out_oaid")
//

        val rdd = sc.textFile("/data/spark/ssp/imei_idfa_oaid/out_disticnt")

        val rdd2 = rdd.mapPartitions(iter => {
            iter.map(line => {
                line.replace("(","")
                    .replace(")","")
                    .split(",")
            })
        }).cache

        rdd2.filter(_(0) == "idfa")
            .map(_(1))
            .saveAsTextFile("/data/spark/ssp/imei_idfa_oaid/out_idfa")


        rdd2.filter(_(0) == "oaid")
            .map(_(1))
            .saveAsTextFile("/data/spark/ssp/imei_idfa_oaid/out_oaid")


        //

        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._
        //  0c4e3e629b|1083#8,25,1317#11,8,106
        val rdd3 = sc.textFile("/data/spark/ssp_customer/7_unicom_id/data/2021-05-27")
        rdd3.flatMap(line => {
            val arr = line.split("\\|")

            val idFront = arr(0)
            val details = arr(1)

            val arr2 = details.split("#")

            val idType = arr2(0).substring(0,1)
            val provid = arr2(0).substring(1)  // 083

            arr2.slice(1,arr2.length).map(str => {
                val urlDetailArr = str.split(",")
                val urlId = urlDetailArr(0)
                val count1 = urlDetailArr(1).toInt
                val count2 = urlDetailArr(2).toInt
                idType match {
                    case "1" => (idFront, idType, provid, urlId, count1, count2)
                    case "0" => (idFront, idType, provid, urlId, count2, count1)
                }

            })

        }).toDF("id_front","id_type","prov_id","url_id","freq", "dura")
            .createOrReplaceTempView("hobby_table")

        spark.sql(
            s"""
               |select
               |    t3.imei,-- imei
               |    t2.name, -- 省份
               |    t1.id_type, --  >5: 1  >30s: 0
               |    t1.url_id,
               |    t1.freq,
               |    t1.dura,
               |    t4.url_rule
               |from
               |    hobby_table t1  -- 下载下来的文件映射成的表
               |left join
               |    ods_custom.dim_unicom_dmp_province t2 --省份维表
               |on t1.prov_id = t2.id
               |left join
               |    ( -- 最大的日期
               |    select
               |       imei
               |    from
               |    ods_custom.unicom_dmp_input_imei   -- 上传到联通的imei文件
               |    where etl_date in (select max(etl_date) from ods_custom.unicom_dmp_input_imei)
               |    ) t3
               |on  substr(t3.imei,0,10) = t1.id_front
               |left join
               |(
               |    select
               |    id,url_rule
               |    from
               |    ods_custom.unicom_url_rule
               |    where etl_date in (select max(etl_date) from ods_custom.unicom_url_rule)
               |) t4
               |on t1.url_id = t4.id
               |
               |""".stripMargin).rdd
            .map(row => row.mkString(","))
            .saveAsTextFile("")




    }
}
