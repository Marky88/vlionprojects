package com.advlion.www.sync

import com.advlion.www.jdbc.MySQLJdbc
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object ImportData {
  private val url: String = MySQLJdbc.url
  private val user: String = MySQLJdbc.user
  private val password: String = MySQLJdbc.password
//  private val driver: String = MySQLJdbc.driver
  private val uri: String = url + "?user=" + user + "&password=" + password

  def importMySQL(spark:SparkSession): Unit = {
    val adsLocationDF = spark.read.format("jdbc")
      .options(Map("url" -> uri, "dbtable" -> "adslocation"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val creativeDF = spark.read.format("jdbc")
      .options(Map("url" -> uri, "dbtable" -> "creative"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val planDF = spark.read.format("jdbc")
      .options(Map("url" -> uri, "dbtable" -> "plan"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val userDF = spark.read.format("jdbc")
      .options(Map("url" -> uri, "dbtable" -> "user"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    adsLocationDF.createOrReplaceTempView("adsLocation")
    creativeDF.createOrReplaceTempView("creative")
    planDF.createOrReplaceTempView("plan")
    userDF.createOrReplaceTempView("user")

  }
}
