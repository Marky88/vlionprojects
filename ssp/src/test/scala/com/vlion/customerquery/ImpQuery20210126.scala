package com.vlion.customerquery

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @description:取 11月1号到1月 的媒体返回日志，取dspid/sspid=193的 imei/idfa
 *                到1月11日
 *
 *                这样吧，先跑一版曝光的，需要关联的后面慢慢跑
 * @author: malichun
 * @time: 2021/1/26/0026 14:44
 *
 */
object ImpQuery20210126 extends App {
    val spark = SparkSession
        .builder()
        //     .master("local[*]")
        .appName("Ssp")
        .config("hive.metastore.uris", "trift://www.bigdata02.com:9083")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
        .config("spark.debug.maxToStringFields", "100")
        .enableHiveSupport()
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val sc = spark.sparkContext

//    val rdd = sc.textFile("/tmp/test/20210126_media_resp/ssp_imp/2021-01-11/storm_new_ssp_imp_vlionserver-73_2021-01-11_22.log")
    val rdd = sc.textFile("/tmp/test/20210126_media_resp/ssp_imp/*/*")
    rdd.filter(line => line != null && line != "").mapPartitions(iter => {
        iter.filter(_ != null).map(_ split "\t" ).filter(_.length > 30).map(arr => {
            (arr(3), arr(21), arr(14), arr(15)) // dspId, type, imei, idfa
        }) collect {
            case ("193", dtype, imei, idfa) if ((dtype == "1" && imei != null && imei != "000000000000000" && imei != "")
                || (dtype == "2" && idfa != null && idfa != "")) =>
                val value = dtype match {
                    case "1" => imei
                    case "2" => idfa
                }
                dtype+"\t"+value
        }
    }).distinct()
       .saveAsTextFile("/tmp/test/20210126_media_resp/ssp_imp/tmp")

        sc.textFile("/tmp/test/20210126_media_resp/ssp_imp/tmp")
        .map(line => {
            val arr = line.split("\t")
            ((arr(0),scala.util.Random.nextInt(10)),arr(1))
        })
        .groupByKey()
        .foreach { case (key, iter: Iterable[String]) =>

            val type_ = key._1 match {
                case "1" => "imei"
                case "2" => "idfa"
            }
            println(raw"处理${type_}")
            //写入hdfs
            val configuration = new Configuration()
            val fs = FileSystem.get(configuration)
            val outputStream = fs.create(new Path(s"hdfs://www.bigdata02.com:8020/tmp/test/20210126_media_resp/${type_}_imp/${type_}_${key._2}.txt"))
            val bufferWriter = new BufferedWriter(new OutputStreamWriter(outputStream))
            iter.foreach(line => {
                if (line != null) {
                    bufferWriter.write(line)
                    bufferWriter.newLine()
                    bufferWriter.flush()
                }
            })
            bufferWriter.close()
            outputStream.close()
            fs.close()
        }

}
