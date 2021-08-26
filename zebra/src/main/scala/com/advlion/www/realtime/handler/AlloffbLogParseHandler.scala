package com.advlion.www.realtime.handler

import java.sql.Connection
import java.text.SimpleDateFormat

import com.advlion.www.realtime.model.{Alloffb_log, URL_param}
import com.advlion.www.realtime.util.JdbcUtil
import org.apache.spark.streaming.dstream.DStream

/**
 * 统计任务说明：
 *
 * 时间（小时维度）、用户ID、设备类型、SDK版本号、广告位编号、请求次数、成功次数、失败次数、曝光次数
 *
 * @date 2020/7/27 002711:50
 */
object AlloffbLogParseHandler extends Serializable {

    // 生成目标RDD
    //统计任务说明：
    //
    //时间（小时维度）、用户ID、设备类型、SDK版本号、广告位编号、请求次数、成功次数、失败次数、曝光次数
    //"120.36.224.120"    -    [22/Jul/2020:00:08:30 -0700]"PUT /api/v1/adsync?ad=217726882693530_217727316026820&appkey=6352e2c40c14639e&event=0&msg=&os=android&ts=1595401705&uid=fadeb78d-ca92-4e1d-bde8-2aa20e45df6b&ver=5.9.0&sign=64c9ef0e09fa351f4ea63833030d91b6 HTTP/1.1"  200 0.000   32    "-"    "Dalvik/2.1.0 (Linux; U; Android 10; Redmi Note 8 Pro MIUI/V11.0.3.0.QGGCNXM)"    "-"
    def addBatchReport(alloffbLogDStream:DStream[Alloffb_log]):Unit={
         alloffbLogDStream
            .map(log => ((log.date, log.hour, log.url.uid,log.url.os, log.url.ver, log.url.ad, log.url.event),1L))
            .reduceByKey(_+_)
            .map{case ((date, hour,uid, os,ver,ad,event),count) => ((date, hour, uid,os,ver,ad),(event,count))}
            .groupByKey()
            .map(tuple => {
                val value1 = tuple._2
                val map = value1.toMap
                (tuple._1._1,tuple._1._2,tuple._1._3,tuple._1._4,tuple._1._5,tuple._1._6,map.getOrElse("0",0L),map.getOrElse("1",0L),map.getOrElse("2",0L),map.getOrElse("3",0L))
            })
        //往mysql中塞入数据
        .foreachRDD(rdd => {
            rdd.foreachPartition(iter => {
                val connection: Connection = JdbcUtil.getConnection
                iter.foreach{ case (day,hour,uid,os,ver,ad,reqNum,successNum,failNum,impCnt) => {
                    JdbcUtil.executeUpdate(connection,
                        s"""
                          |insert into fb_log_report(dt,hour,uid,os,ver,ad,req_num,success_num,fail_num,imp_cnt)
                          |values('${day}','${hour}','${uid}','${os}','${ver}','${ad}',${reqNum},${successNum},${failNum},${impCnt})
                          |on duplicate key
                          |update req_num=req_num+${reqNum},success_num=success_num+${successNum},fail_num=${failNum},imp_cnt=imp_cnt+${impCnt}
                          |""".stripMargin, Array())
                }}
                connection.close()

            })
        })

    }


}
