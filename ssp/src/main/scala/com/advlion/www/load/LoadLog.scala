package com.advlion.www.load

import com.advlion.www.log.SspImp
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object LoadLog {
  def readLog(spark: SparkSession,args: Array[String]): Unit = {
    val etlDate = "2020-04-24"
    val etlHour = "10"
    //    val etlDate = args(0).toString
    //    val etlHour = args(1).toString
    println(s"/data/ssp/ssp_imp/$etlDate/storm_new_ssp_imp_*_${etlDate}_$etlHour.log")

    val sspImpRDD = spark.sparkContext
      .textFile(s"/data/ssp/ssp_imp/$etlDate/storm_new_ssp_imp_*_${etlDate}_$etlHour.log")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val sspClkRDD = spark.sparkContext
      .textFile(s"/data/ssp/ssp_clk/$etlDate/storm_new_ssp_clk_*_${etlDate}_$etlHour.log")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val dspRespRDD = spark.sparkContext
      .textFile(s"/data/ssp/dsp_resp/$etlDate/storm_new_dsp_resp_*_${etlDate}_$etlHour.log")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val mediaReqRDD = spark.sparkContext
      .textFile(s"/data/ssp/media_req/$etlDate/storm_new_media_req_*_${etlDate}_$etlHour.log")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val mediaReqFilterRDD = spark.sparkContext
      .textFile(s"/data/ssp/media_req_filter/$etlDate/storm_new_media_req_filter_*_${etlDate}_$etlHour.log")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val mediaRespRDD = spark.sparkContext
      .textFile(s"/data/ssp/media_resp/$etlDate/storm_new_media_resp_${etlDate}_$etlHour.log")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val sendDspRes = spark.sparkContext
      .textFile(s"/data/ssp/send_dsp_res/$etlDate/storm_new_send_dsp_res_${etlDate}_$etlHour.log")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val sspDpRDD = spark.sparkContext
      .textFile(s"/data/ssp/ssp_dp/$etlDate/storm_new_ssp_dp_${etlDate}_$etlHour.log")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    println(sspImpRDD)
    import spark.implicits._

    val sspImpDS = sspImpRDD.map(line => {
      val row = line.split("\\t")
//      SspImp(row(1), row(3), row(4), row(5)
        SspImp(row(1), row(3), row(4), row(5),row(11),row(14),row(15)
      )
    }).toDS()
    sspImpDS.createOrReplaceTempView("sspImp")
    spark.sql("select count(1) from sspImp").show()
    spark.sql("select * from sspImp").show()

  }
}
