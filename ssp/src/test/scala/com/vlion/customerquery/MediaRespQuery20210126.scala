package com.vlion.customerquery

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/1/26/0026 14:21
 *
 *        取 11月1号到1月 的媒体返回日志，取dspid/sspid=193的 imei/idfa
 *        到1月11日
 *
 *        imei,idfa要从媒体请求日志中拿过来
 *        这样吧，先跑一版曝光的，需要关联的后面慢慢跑
 */
object MediaRespQuery20210126 {
    def main(args: Array[String]): Unit = {
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

        val etlDate = args(0)

        val sc = spark.sparkContext

        //媒体返回
        val rddMediaResp = sc.textFile(s"/tmp/test/20210126_media_resp/media_resp/${etlDate}") //每天的中间文件
        val rddMediaResp2 = rddMediaResp.filter(line => line!=null && line.split("\t").length > 10).map(line => {
            val arr = line.split("\t")
            val reqId = arr(5)
            val dspId = arr(7)
            (reqId, dspId)
        }).filter(_._2 == "193")

        //媒体请求
        val rddMediaReq = sc.textFile(s"/tmp/test/20210126_media_resp/media_req/${etlDate}")
        val rddMediaReq2 = rddMediaReq.filter(line => line!=null && line.split("\t").length > 15).map(line => {
            val arr = line.split("\t")
            val reqId = arr(5)
            val dtype = arr(11)
            val imei = arr(7)
            val idfa = arr(10)
            (reqId, dtype, imei, idfa)
        })
            .filter{case (reqId, dtype, imei, idfa) => ((dtype == "1" && imei != null && imei != "000000000000000" && imei != "")
                || (dtype == "2" && idfa != null && idfa != "")) }
            .map{case (reqId, dtype, imei, idfa) =>
                val value = dtype match {
                    case "1" => imei
                    case "2" => idfa
                }
                (reqId,(dtype, value))
            }


        val joinRdd = rddMediaResp2.join(rddMediaReq2).distinct

        joinRdd.map{case (_,(_,(dtype,id))) =>
            dtype+"\t"+id
        }.coalesce(10)
            .saveAsTextFile(s"/tmp/test/20210126_media_resp/media_res_result/${etlDate}")



        //                .groupByKey()
        //                .foreach{case (key: String,iter: Iterable[String]) =>
        //                    val type_ = key match {
        //                        case "1" => "imei"
        //                        case "2" => "idfa"
        //                    }
        //
        //                    //写入hdfs
        //                    val configuration = new Configuration()
        //                    val fs = FileSystem.get(configuration)
        //                    val outputStream = fs.create(new Path(s"/tmp/test/20210126_media_resp/${type_}/${etlDate}.txt"))
        //                    val bufferWriter = new BufferedWriter(new OutputStreamWriter(outputStream))
        //                    iter.foreach(line => {
        //                        if (line != null) {
        //                            bufferWriter.write(line)
        //                            bufferWriter.newLine()
        //                            bufferWriter.flush()
        //                        }
        //                    })
        //                    bufferWriter.close()
        //                    outputStream.close()
        //                    fs.close()
        //                }

    }
}
