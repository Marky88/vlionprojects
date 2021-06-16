package sync

import com.advlion.www.jdbc.MySQL
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object ImportData2 {
  val url = "jdbc:mysql://172.16.189.204:3306/test"
  val user = MySQL.user
  val password = MySQL.password
  //val url = "jdbc:mysql://172.16.189.204:3306/hippo?user=VLION_HIPPO&password=VLION_HIPPO"  //mammu
  val uri = url + "?user=" + user + "&password=" + password

  def importMySQL(spark:SparkSession): Unit ={
    val mediaDF = spark.read.format("jdbc")
      .options(Map("url" -> uri, "dbtable" -> "media"))
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    mediaDF.createOrReplaceTempView("adslocation")
    spark.sql("insert overwrite  table default.adslocation select * from adslocation")

  }





}
