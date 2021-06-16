package com.advlion.www.task

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter

import com.advlion.www.analysis.{CheapData, OfflineStatistics, SspReport}
import com.advlion.www.load.{LoadHive, LoadLog}
import com.advlion.www.sync.ImportData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Ssp {
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
//    LoadLog.readLog(spark: SparkSession,args: Array[String])
    try {
        ImportData.importMySQL(spark: SparkSession)

        LoadHive.readHive(spark: SparkSession, args: Array[String]) //读取hive的表数据,生成summary view
        LoadHive.hiveDau(spark: SparkSession, args: Array[String]) //dau
        OfflineStatistics.summary(spark: SparkSession, args: Array[String]) //将数据导入到mysql表stat_day

        if(args(1) == "23"){  //如果是23小时计算一次
            OfflineStatistics.dauStatistics(spark: SparkSession) //将数据导入到 mysql表ssp_dau_tmp 先取消
        }

        OfflineStatistics.dspCreativeStatistics(spark: SparkSession, args: Array[String]) //直接通过hive处理导入到mysql stat_channel_creative
        OfflineStatistics.dspCreativeStatisticsNew(spark, args) //直接通过hive处理导入到mysql stat_channel_creative_new
        OfflineStatistics.dspCreativeStatisticsImg(spark,args) //解析json得到图片img,导入到mysql:creative_img

//        CheapData.cheapAnalysis(spark: SparkSession, args: Array[String])
        CheapData.cheapAnalysis2(spark,args(0),args(1)) //20201110修改现有反作弊

        OfflineStatistics.summaryMediaPkg(spark, args) //20200624 增加媒体包名统计报表

        //20200710 ssp报告数据生成到hive表default.ssp_report
        SspReport.analysis(spark,args(0),args(1))

      /* 2020-12-30 取消
       //20201215 每3小时媒体请求,拿到imei,idfa,oaid的一些设备,根据id生成文件,为下游拿到接口数据,塞入ssdb
        if( (args(1).toInt + 1 ) % 3  == 0){  // 2,5,8,11,.....,23
            OfflineStatistics.mediaReqThreeHourGenerate(spark,args(0),args(1))
        }
        */
        //20201215同上, 每1小时再跑曝光日志dspid=79的，取出设备ID请求，如果没有label，则把对应的key里面的field从ssdb中删除: hdel(key, field)
//        OfflineStatistics.impHourGenerate(spark,args(0),args(1))


    }catch {
        case e:Exception => e.printStackTrace()
            sys.exit(4)
    }finally {
        spark.stop()
        spark.sparkContext.stop()
    }
}
}
