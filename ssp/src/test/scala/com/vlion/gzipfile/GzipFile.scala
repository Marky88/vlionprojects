package com.vlion.gzipfile

import java.util.zip.{GZIPOutputStream, ZipEntry, ZipOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @description:  文件在hdfs上面压缩,压缩那块可以用个Java设计模式
 * @author: malichun
 * @time: 2021/3/12/0012 10:39
 *
 */
object GzipFile {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession
            .builder()
            //     .master("local[*]")
            .appName("Ssp")
            .config("hive.metastore.uris","trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir","/user/hive/warehouse")
            .config("spark.debug.maxToStringFields","100")
            .enableHiveSupport()
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._

        val allColumns = spark.read.table("ods.ods_media_req").columns
        val columns = allColumns.slice(0,allColumns.length-2)
//        val inputDFTmp = spark.sql(
//            """
//              |select * from
//              |ods.ods_media_req
//              |where etl_date='2021-03-12' and etl_hour='06'
//              |""".stripMargin)
//        val columns = inputDFTmp.columns.slice(0,inputDFTmp.columns.length-2)

        val inputDF = spark.sql(
            s"""
              | select
              | concat(${columns.mkString(",'\t',")}) as cols
              | from
              | ods.ods_media_req
              |where etl_date='2021-03-12' and etl_hour='06'
              |""".stripMargin)

        val dataRdd = inputDF.rdd.map(row => row.getAs[String]("cols"))

        val lineCount = dataRdd.count()

        val splitCount = (lineCount/5000000 + 1) toInt

        dataRdd.repartition(splitCount)
            .mapPartitionsWithIndex((index,iter) => {
                compress_split(index,iter,"/tmp/test/20210312_test_file/file")
            })
            .repartition(1)
            .mapPartitions(iter => {
                zipFile(iter, "/tmp/test/20210312_test_file/out.zip")
            })
            .collect



    }

    def zipFile(filePaths: Iterator[String],outputFileName:String):Iterator[String]={
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        val path = new Path(outputFileName)
        val outputStream = fs.create(path)

        val zipOut = new ZipOutputStream(outputStream)
        filePaths.foreach(inputPath => {
            zipOut.putNextEntry(new ZipEntry(inputPath.split("/")(4)))
            //每个输入文件处理
            val inputStream = fs.open(new Path(inputPath))
            val bytes = Array.ofDim[Byte](1024)

            var flag = true
            while(flag){
                val count = inputStream.read(bytes)
                if(count == -1){
                    flag = false
                }else{
                    zipOut.write(bytes,0,count)
                }
            }
            inputStream.close()
        })
        zipOut.close()
        outputStream.close()
        fs.close()
        Array(outputFileName).toIterator
    }

    def compress_split(index:Int,iter:Iterator[String],outputBasePath:String) : Iterator[String] ={
        val outputFileName = outputBasePath + "_" + index + ".gz"
        println(s"生成$outputFileName")
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        val path = new Path(outputFileName)
        val outputStream = fs.create(path)

        val gzip = new GZIPOutputStream(outputStream)

        val lineSeparator = "\n".getBytes("utf-8")
        iter.foreach( line => {
            if(line != null && line != ""){
                gzip.write(line.getBytes("utf-8"))
                gzip.write(lineSeparator)
            }
        })

        gzip.close()
        outputStream.close()
        fs.close()

        //返回输出
        Array(outputFileName).toIterator
    }

}
