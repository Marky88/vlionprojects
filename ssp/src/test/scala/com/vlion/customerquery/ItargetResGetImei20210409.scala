package com.vlion.customerquery

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * @description:
 * @author: malichun
 * @time: 2021/4/9/0009 13:37
 *
 *        人群文件的id替换imei
 */
object ItargetResGetImei20210409 {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("123"))
        val spark = SparkSession.builder().getOrCreate()

        import spark.implicits._

        val originInputRDD = sc.textFile("/tmp/test/20210409_itarget_change_imei/input")


        val resOriginRDD = sc.wholeTextFiles("/tmp/test/20210409_itarget_change_imei/res_origin")
        val resC = resOriginRDD.collect()


        val map = resC.foldLeft(mutable.Map[String, mutable.Set[String]]())((m, e) => {
            val filePath = e._1
            val content = e._2
            val arr = filePath.split("/")
            val key = arr(arr.length - 1)

            val set = (mutable.Set[String]() /: content.split("\n")) ((s, line) => {
                if (line != null && line != "") {
                    val arr1 = line.split("\\|")
                    s.add(arr1(0))
                }
                s
            })
            m.put(key, set)
            m
        })


        val broadcastVar = sc.broadcast(map)

        val pr = originInputRDD.mapPartitions(iter => {
            val map = broadcastVar.value
            val keys = map.keys

            iter.collect { case line if line != null && line != "" =>
                val arr = line.split("\\|")
                val id = arr(0)
                val imei = arr(1)
                keys.collect { case key if map(key).contains(id) =>
                    (key, imei)
                }
            }.flatten
        })

        val res2 = pr.groupByKey().collect()

        res2.foreach { case (s,iter) =>
            val bw = new BufferedWriter(new FileWriter(new File("/home/spark/ssp/customer_query/20210331_imie_idfa_day/20210409_res_imei_change/"+ s)))
            iter.foreach(line => {

                bw.write(line)
                bw.newLine()
                bw.flush()
            })
            bw.close()
        }


    }


}
