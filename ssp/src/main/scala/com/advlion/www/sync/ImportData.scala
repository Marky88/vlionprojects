package com.advlion.www.sync


import com.advlion.www.jdbc.MySQL
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object ImportData {
  def importMySQL(spark:SparkSession): Unit ={
    val url = MySQL.url
    val user = MySQL.user
    val password = MySQL.password
    //val url = "jdbc:mysql://172.16.189.204:3306/hippo?user=VLION_HIPPO&password=VLION_HIPPO"  //mammu
    val uri = url + "?user=" + user + "&password=" + password

    val mediaDF = spark.read.format("jdbc")
      .options(Map("url" -> uri, "dbtable" -> "media"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val adsLocationDF = spark.read.format("jdbc")
      .options(Map("url" -> uri, "dbtable" -> "adslocation"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val userMediaMapDF = spark.read.format("jdbc")
      .options(Map("url" -> uri, "dbtable" -> "user_media_map"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

  //20200714 ssp_report报告新增4张表,置换日志id
    val cutOsDF = spark.read.format("jdbc")
        .options(Map("url" -> uri,"dbtable"-> "cut_os"))
        .load()
        .cache()
    val cutNetWorkDF = spark.read.format("jdbc")
      .options(Map("url" -> uri,"dbtable"-> "cut_network"))
      .load()
      .cache()
    val cutWifiDF = spark.read.format("jdbc")
      .options(Map("url" -> uri,"dbtable"-> "cut_wifi"))
      .load()
      .cache()
    val cutBrandDF = spark.read.format("jdbc")
      .options(Map("url" -> uri,"dbtable"-> "cut_brand"))
      .load()
      .cache()


    mediaDF.createOrReplaceTempView("media")
    adsLocationDF.createOrReplaceTempView("adsLocation")
    userMediaMapDF.createOrReplaceTempView("userMediaMap")

    cutOsDF.createOrReplaceTempView("cut_os")
    cutNetWorkDF.createOrReplaceTempView("cut_network")
    cutWifiDF.createOrReplaceTempView("cut_wifi")
    cutBrandDF.createOrReplaceTempView("cut_brand")

//    val basicDF = spark.sql(
//      """
//        |select a.id as adsolt_id,
//        |		a.joint_id,
//        |		a.deduct,
//        |		m.user_id as dev_id,
//        |		u.user_id
//        |from adsLocation a
//        |inner join media m
//        |on		a.media_id  = m.id
//        |inner  join userMediaMap u
//        |on		u.media_id  = m.id
//        |""".stripMargin)
//    basicDF.createOrReplaceTempView("basic")
  }
}
