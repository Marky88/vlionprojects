package com.advlion.www.sync

import com.advlion.www.jdbc.MySQL
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object ImportData {
  val url: String = MySQL.url
  val user: String = MySQL.user
  val password: String = MySQL.password
  val uri: String = url + "?user=" + user + "&password=" + password
  def importMySQL(spark: SparkSession): Unit ={
    val mediaDF = spark.read.format("jdbc")
      .options(Map("url"->uri,"dbtable"->"media"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK)
    val planDF = spark.read.format("jdbc")
      .options(Map("url"->uri,"dbtable"->"plan"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK)
    val adslocationDF = spark.read.format("jdbc")
      .options(Map("url"->uri,"dbtable"->"adslocation"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK)

    mediaDF.createOrReplaceTempView("media")
    planDF.createOrReplaceTempView("plan")
    adslocationDF.createOrReplaceTempView("adslocation")
    mediaDF.show()
  }
}
