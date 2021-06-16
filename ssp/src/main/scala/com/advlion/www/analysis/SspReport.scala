package com.advlion.www.analysis

import com.advlion.www.util.CleanDataUDF
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 20200710
 * 解析,生成报告数据,塞入到hive表default.ssp_report表
 *
 *
 *  维度:
 *
 *
 *  日期,
 *  小时
 *  媒体
 *  代码位
 *  渠道,
 *  操作系统,
 *  运营商,
 *  网络连接类型,
 *  设备品牌,     producer
 *  地域
 *
 *
 *
 *  指标:
 *  有效请求，广告返回，展示，点击，CPM
 *
 *
 *  填充率，展示率，点击率
 */
object SspReport {
    val kuduMaster="www.bigdata03.com:7051"
    val kuduTable = "impala::dm.ssp_report_test"
    val reportTable = "dm.ssp_report"

    def analysis(spark:SparkSession,etlDate:String,etlHour:String): Unit ={
        spark.udf.register("clean_producer",CleanDataUDF.parseBrand _)
        //媒体请求
        val mediaReqDF=spark.sql(
            s"""
               |select
               |app_id,
               |adsolt_id,
               |null as dsp_id,
               |os,
               |carrier,
               |network,
               |clean_producer( producer) as producer , --设备类型
               |city_id,
               |count(1) as req_cnt,
               |0 as resp_cnt,
               |0 as imp_cnt ,
               |0 as clk_cnt ,
               |0 as cpm_sum ,
               |0 as cpm_cnt,
               |0 as floor_price_sum,
               |0 as floor_price_cnt
               |from
               |ods.ods_media_req
               |where etl_date='$etlDate' and etl_hour='$etlHour'
               |group by
               |app_id,
               |adsolt_id,
               |os,
               |carrier,
               |network,
               |clean_producer( producer)   , --设备类型
               |city_id
               |""".stripMargin)

        //媒体返回
        val mediaRespDF=spark.sql(
            s"""
               |select
               |app_id,
               |adsolt_id,
               |dsp_id,
               |os,
               |carrier,
               |network,
               |clean_producer( producer) as producer , --设备类型
               |city_id  ,
               |0 as  req_cnt,
               |count(1) as resp_cnt,
               |0 as imp_cnt ,
               |0 as clk_cnt ,
               |0 as cpm_sum ,
               |0 as cpm_cnt,
               |0 as floor_price_sum,
               |0 as floor_price_cnt
               |from
               |ods.ods_media_resp
               |where status_code = '0'  -- 0是有广告
               |and etl_date='$etlDate' and etl_hour='$etlHour'
               |group by
               |app_id,
               |adsolt_id,
               |dsp_id,
               |os,
               |carrier,
               |network,
               |clean_producer( producer)   , --设备类型
               |city_id
               |""".stripMargin)


        //曝光
        val impDF = spark.sql(
            s"""
               |select
               |app_id,
               |adsolt_id,
               |dsp_id, -- 渠道id
               |os,
               |carrier,
               |network,
               |clean_producer( producer) as producer , --设备类型
               |city_id ,
               |0 as req_cnt  ,
               |0 as resp_cnt ,
               |count(1) as imp_cnt   ,
               |0 as clk_cnt  ,
               |sum(real_price) as cpm_sum,
               |count(real_price) as cpm_cnt,
               |sum(floor_price) as floor_price_sum,
               |count(floor_price) as floor_price_cnt
               |from
               |ods.ods_ssp_imp
               |where  etl_date='$etlDate' and etl_hour='$etlHour'
               |group by
               |app_id,
               |adsolt_id,
               |dsp_id, -- 渠道id
               |os,
               |carrier,
               |network,
               |clean_producer( producer) , --设备类型
               |city_id
               |
               |""".stripMargin)


        //点击
        val clkDF = spark.sql(
            s"""
               |-- 点击
               |select
               |app_id,
               |adsolt_id,
               |dsp_id, -- 渠道id
               |os,
               |carrier,
               |network,
               |clean_producer( producer) as producer , --设备类型
               |city_id ,
               |0 as req_cnt   ,
               |0 as resp_cnt  ,
               |0 as imp_cnt   ,
               |count(1) as clk_cnt     ,
               |0 as cpm_sum   ,
               |0 as cpm_cnt,
               |0 as floor_price_sum,
               |0 as floor_price_cnt
               |from
               |ods.ods_ssp_clk
               |where  etl_date='$etlDate' and etl_hour='$etlHour'
               |group by
               |app_id,
               |adsolt_id,
               |dsp_id, -- 渠道id
               |os,
               |carrier,
               |network,
               |clean_producer( producer) , --设备类型
               |city_id
               |
               |""".stripMargin)

        val resDF=mediaReqDF.union(mediaRespDF).union(impDF).union(clkDF)
            .groupBy("app_id", "adsolt_id", "dsp_id", "os", "carrier", "network", "producer", "city_id")
            .sum("req_cnt", "resp_cnt", "imp_cnt", "clk_cnt", "cpm_sum", "cpm_cnt","floor_price_sum","floor_price_cnt")
            .toDF("app_id", "adsolt_id", "dsp_id", "os", "carrier", "network", "producer", "city_id",
                "req_cnt", "resp_cnt", "imp_cnt", "clk_cnt", "cpm_sum", "cpm_cnt","floor_price_sum","floor_price_cnt")
            .repartition(1).toDF

        resDF.createOrReplaceTempView("res_tab")


//数据写入到hvie表
        spark.sql("set spark.sql.shuffle.partitions=50")
        spark.sql(
            s"""insert overwrite table
               |${reportTable}
               |partition(etl_date='$etlDate',etl_hour='$etlHour')
               |
               |select
               |t1.app_id,
               |t1.adsolt_id,
               |t1.dsp_id,
               |t2.id as os,
               |t3.id as carrier,
               |t4.id as network,
               |t5.id as producer,
               |t1.city_id,
               |t1.req_cnt     ,
               |t1.resp_cnt      ,
               |t1.imp_cnt        ,
               |t1.clk_cnt        ,
               |t1.cpm_sum        ,
               |t1.cpm_cnt        ,
               |t1.floor_price_sum,
               |t1.floor_price_cnt
               |from
               |res_tab t1
               |left join
               |cut_os t2 on t1.os=t2.jsonkey
               |left join
               |cut_network t3 on t1.carrier = t3.jsonkey
               |left join
               |cut_wifi t4 on t1.network=t4.jsonkey
               |left join
               |cut_brand t5  on t1.producer = t5.jsonkey
               |
               |""".stripMargin)
        spark.sql("set spark.sql.shuffle.partitions=500") //恢复默认分区

        //删除报告表 过期分区
//        dropReportPartitions(spark,etlDate) //会有问题



//数据写入到kudu
//构建 KuduContext 对象
//        val sc = spark.sparkContext
//        //构建kuduContext对象
//        val kuduContext = new  KuduContext(kuduMaster,sc)
//
//        import spark.implicits._
//        kuduContext.upsertRows(resDF,kuduTable)


        //TODO 导入到ssp_report_ua,看每天效率
//        spark.sql(
//            s"""
//               |
//               |insert overwrite table dm.ssp_report_ua partition(etl_date='${etlDate}',etl_hour='${etlHour}')
//               |select
//               |app_id,
//               |adsolt_id,
//               |user_agent as ua,
//               |imei,
//               |ip,
//               |sum(req_cnt) as req_cnt,
//               |sum(imp_cnt) as imp_cnt,
//               |sum(clk_cnt) as clk_cnt
//               |from
//               |(
//               |select
//               |app_id,
//               |adsolt_id,
//               |user_agent ,
//               |imei,
//               |ip,
//               |count(1) as req_cnt,
//               |0 as imp_cnt,
//               |0 as clk_cnt
//               |from
//               |ods.ods_media_req
//               |where etl_date='${etlDate}' and etl_hour='${etlHour}'
//               |group by
//               |app_id,
//               |adsolt_id,
//               |user_agent,
//               |imei,
//               |ip
//               |union all
//               |select
//               |app_id,
//               |adsolt_id,
//               |user_agent,
//               |imei,
//               |ip,
//               |0 as req_cnt,
//               |count(1) as imp_cnt,
//               |0 as clk_cnt
//               |from
//               |ods.ods_ssp_imp
//               |where etl_date='${etlDate}' and etl_hour='${etlHour}'
//               |group by
//               |app_id,
//               |adsolt_id,
//               |user_agent,
//               |imei,
//               |ip
//               |union all
//               |select
//               |app_id,
//               |adsolt_id,
//               |user_agent,
//               |imei,
//               |ip,
//               |0 as req_cnt,
//               |0 as imp_cnt,
//               |count(1) as clk_cnt
//               |from
//               |ods.ods_ssp_clk
//               |where etl_date='${etlDate}' and etl_hour='${etlHour}'
//               |group by
//               |app_id,
//               |adsolt_id,
//               |user_agent,
//               |imei,
//               |ip
//               |
//               |) t
//               |group by
//               |app_id,
//               |adsolt_id,
//               |user_agent,
//               |imei,
//               |ip
//               |""".stripMargin)


    }

//    /**
//     * 删除dm.ssp_report表的分区
//     * @param etlDate 当前时间
//     */
//    def dropReportPartitions(spark:SparkSession,etlDate:String):Unit={
//        //删除90天前的分区
//        import java.text.SimpleDateFormat
//        import java.util.Calendar
//        val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
//
//        val calendar1 = Calendar.getInstance
//        calendar1.setTime(sdf1.parse(etlDate))
//        calendar1.add(Calendar.DATE, -90)
//        val dropDate = sdf1.format(calendar1.getTime)
//
//        spark.sql(s"alter table ${reportTable} drop partition(etl_date <='${dropDate}') ")
//
//    }


}
