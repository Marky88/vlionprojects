package com.vlion.gzipfile

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.spark.input.PortableDataStream

import scala.util.Try
import java.nio.charset._

import org.apache.spark.storage.StorageLevel

/**
 * @description:
 * @author: malichun
 * @time: 2021/5/7/0007 12:04
 *
 *        spark读取tar gz 文件
 *
 */
object ReadTarGZFile {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf())

        val value = sc.binaryFiles("/data/spark/ssp/imei_idfa_oaid").flatMapValues(x =>
            extractFiles(x).toOption
        ).mapValues(_.map(decode()))
                .persist(StorageLevel.MEMORY_AND_DISK)
        value.take(10)

        sc.stop()
    }


    def extractFiles(ps: PortableDataStream, n: Int = 1024) = Try {
        val tar = new TarArchiveInputStream(new GzipCompressorInputStream(ps.open))
        Stream.continually(Option(tar.getNextTarEntry))
            // Read until next exntry is null
            .takeWhile(_.isDefined).flatten
            // Drop directories
            .filter(!_.isDirectory)
            .map(e => {
                Stream.continually {
                    // Read n bytes
                    val buffer = Array.fill[Byte](n)(-1)
                    val i = tar.read(buffer, 0, n)
                    (i, buffer.take(i))
                }
                    // Take as long as we've read something
                    .takeWhile(_._1 > 0).flatMap(_._2)
                    .toArray
            })
            .toArray
    }

    def decode(charset: Charset = StandardCharsets.UTF_8)(bytes: Array[Byte]) =
        new String(bytes, StandardCharsets.UTF_8)


}
