package com.vlion.spark.query20201207sample

import java.io.{BufferedOutputStream, BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * @description:
 * @author: malichun
 * @time: 2020/12/7/0007 11:27
 *
 *        拿到明确是造假的数据
 *
 *        媒体id
 *        21012
 *        30135
 *        30136
 *        30121
 *        30120
 *        30228
 *        21015
 *        20856
 *        20426
 *        20376
 *
 */
object SampleGet {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .enableHiveSupport()
            .getOrCreate()
        val sc = new SparkContext()


        val etlDate = "2020-12-11"
        val etlHour = "11"

        val tableName = "ods.ods_media_req"

        val columns = spark.catalog.listColumns(tableName)
            .collect()

        val columns1 = columns.map(_.name).slice(0, columns.size - 2)

        val columnsStr = columns1.mkString(",'\t',")


        val concatDF = spark.sql(
            s"""
               |select app_id,concat($columnsStr) as concat_col
               |from ${tableName}
               |where etl_date='${etlDate}' and etl_hour='${etlHour}'
               |""".stripMargin)

        val tupleDF = concatDF.rdd
            .map(r => (r.getString(0), r.getString(1)))
            .persist(StorageLevel.MEMORY_AND_DISK)

        val countRes = tupleDF.map(t => (t._1, 1)).reduceByKey(_ + _).collect //得到每个id有多少条数据  //def reduceByKey(func: (V, V) => V): RDD[(K, V)]
            .map(t => (t._1, t._2 / 60000 + 1)).toMap
        val countBroadMap = sc.broadcast(countRes)


        tupleDF
            .mapPartitions(iter => {
                val countMap = countBroadMap.value
                val sep = "_"
                iter.map { case (key, value) =>
                    (key+sep+scala.util.Random.nextInt(countMap(key)),value)
                }
            } ) //def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U])
            .groupByKey() //RDD[(K, Iterable[V])]
            .foreach {
                case (appId, iter) =>
                    var appId2: String = appId
                    if (
                        appId.split("_")(0) == "20376"
                            || appId.split("_")(0) == "21012"
                            || appId.split("_")(0) == "30135"
                            || appId.split("_")(0) == "30136"
                            || appId.split("_")(0) == "30121"
                            || appId.split("_")(0) == "30120"
                            || appId.split("_")(0) == "30228"
                            || appId.split("_")(0) == "21015"
                            || appId.split("_")(0) == "20856"
                            || appId.split("_")(0) == "20426"
                    ) appId2 += "_cheat"
                    val configuration = new Configuration()
                    val fs = FileSystem.get(configuration)


                    val outputStream = fs.create(new Path(s"/tmp/test/20201207_sample/${tableName}_${appId2}.txt"))
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


}
