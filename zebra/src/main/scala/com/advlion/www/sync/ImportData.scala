package com.advlion.www.sync

import com.advlion.www.jdbc.MySQL
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * 导入mysql数据
 */
object ImportData {
  def importMySQL(spark:SparkSession): Unit ={
    val url = MySQL.url
    val user = MySQL.user
    val password = MySQL.password
    //val url = "jdbc:mysql://172.16.189.204:3306/hippo?user=VLION_HIPPO&password=VLION_HIPPO"
    val uri = url + "?user=" + user + "&password=" + password
    val tagsMapDF = spark.read.format("jdbc")
      .options(Map("url" -> uri, "dbtable" -> "tags_map"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK)
    val urlDF = spark.read.format("jdbc")
      .options(Map("url" -> uri, "dbtable" -> "url"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK)
    val channelIdMapDF = spark.read.format("jdbc")
        .options(Map("url"->uri,"dbtable"->"channel_id_map"))
        .load()
        .persist(StorageLevel.MEMORY_AND_DISK)
    val linkDF = spark.read.format("jdbc")
        .options(Map("url"->uri,"dbtable"->"link"))
        .load()
        .persist(StorageLevel.MEMORY_AND_DISK)


    tagsMapDF.createOrReplaceTempView("tagsMap")
    urlDF.createOrReplaceTempView("url")
    channelIdMapDF.createOrReplaceTempView("channelIdMap")
    linkDF.createOrReplaceTempView("link")

    val tagsMapLinkDF = spark.sql(
      """
        |select a.id as tags_map_id,
        |		a.tags,
        |		a.link_id,
        |		a.tags_md5,
        |		b.url_id,
        |		b.user_id,
        |		b.station_id,
        |       b.channel_id
        |from	tagsMap a
        |left	join link b
        |on		a.link_id = b.id
        |""".stripMargin)
    tagsMapLinkDF.createOrReplaceTempView("tagsMapLink")
  }
}
