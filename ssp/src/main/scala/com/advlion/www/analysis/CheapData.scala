package com.advlion.www.analysis

import com.advlion.www.dao.RedisDAO
import org.apache.spark.sql.{DataFrame, SparkSession}

object CheapData {
  def cheapAnalysis(spark:SparkSession,args: Array[String]): Unit ={
    val etlDate = args(0).toString
    val etlHour = args(1).toString
    /**
     * 点击日志
     */
    val sspClkIpDF = spark.sql(
      s"""
        |select a.ip
        |from
        |	(
        |		select	ip,
        |				count(1) as cn
        |		from 	ods.ods_ssp_clk
        |		where etl_date = '$etlDate'
        |		and 	etl_hour = '$etlHour'
        |		and		ip is not null
        |		and		ip <> ''
        |		group by ip
        |	) a
        |where 	a.cn >= 50
        |""".stripMargin)
    RedisDAO.writeToRedis(sspClkIpDF:DataFrame,"ip":String)

    val sspClkIMeiDF = spark.sql(
      s"""
        |
        |		select	imei
        |		from 	ods.ods_ssp_clk
        |		where etl_date = '$etlDate'
        |		and 	etl_hour = '$etlHour'
        |		and		imei is not null
        |		and		imei <> ''
        |		and		imei <> '000000000000000'
        |		group by imei
        |  having count(1) >=50
        |
        |
        |""".stripMargin)
    RedisDAO.writeToRedis(sspClkIMeiDF:DataFrame,"imei":String)

    val sspClkIdFaDF = spark.sql(
      s"""
        |select 	a.idfa
        |from
        |	(
        |		select	idfa,
        |				count(1) as cn
        |		from 	ods.ods_ssp_clk
        |		where etl_date = '$etlDate'
        |		and 	etl_hour = '$etlHour'
        |		and		idfa is not null
        |		and		idfa <> ''
        |		and		idfa <> '00000000-0000-0000-0000-000000000000'
        |		group by idfa
        |	) a
        |where 	a.cn >= 50
        |""".stripMargin)
    RedisDAO.writeToRedis(sspClkIdFaDF:DataFrame,"idfa":String)

    /**
     * 曝光日志
     */
    val sspImpIpDF = spark.sql(
      s"""
        |select 	a.ip
        |from
        |	(
        |		select	ip,
        |				count(1) as cn
        |		from 	ods.ods_ssp_imp
        |		where etl_date = '$etlDate'
        |		and 	etl_hour = '$etlHour'
        |		and		ip is not null
        |		and		ip <> ''
        |		group by ip
        |	) a
        |where 	a.cn >= 150
        |order 	by a.cn desc
        |""".stripMargin)
    RedisDAO.writeToRedis(sspImpIpDF:DataFrame,"ip":String)

    val sspImpIMeiDF = spark.sql(
      s"""
        |select 	a.imei
        |from
        |	(
        |		select	imei,
        |				count(1) as cn
        |		from 	ods.ods_ssp_imp
        |		where etl_date = '$etlDate'
        |		and 	etl_hour = '$etlHour'
        |		and		imei is not null
        |		and		imei <> ''
        |		and		imei <> '000000000000000'
        |   and   length(imei) = 15
        |   group by imei
        |	) a
        |where 	a.cn >= 150
        |order 	by a.cn desc
        |""".stripMargin)
    RedisDAO.writeToRedis(sspImpIMeiDF:DataFrame,"imei":String)

    val sspImpIdFaDF = spark.sql(
      s"""
        |select 	a.idfa
        |from
        |	(
        |		select	idfa,
        |				count(1) as cn
        |		from 	ods.ods_ssp_imp
        |		where etl_date = '$etlDate'
        |		and 	etl_hour = '$etlHour'
        |		and		idfa is not null
        |		and		idfa <> ''
        |		and 	idfa <> '00000000-0000-0000-0000-000000000000'
        |		group by idfa
        |	) a
        |where 	a.cn >= 150
        |order 	by a.cn desc
        |
        |""".stripMargin)
    RedisDAO.writeToRedis(sspImpIdFaDF:DataFrame,"idfa":String)

    /**
     * 媒体请求
     */
    val  mediaReqIpDF = spark.sql(
      s"""
        |select 	a.ip
        |from
        |	(
        |		select	ip,
        |				  count(1) as cn
        |		from 	ods.ods_media_req
        |		where etl_date = '$etlDate'
        |		and 	etl_hour = '$etlHour'
        |		and		ip is not null
        |		and		ip <> ''
        |		group by ip
        |	) a
        |where 	a.cn >= 300
        |order 	by a.cn desc
        |""".stripMargin)
    RedisDAO.writeToRedis(mediaReqIpDF:DataFrame,"ip":String)

    val mediaReqIMeiDF = spark.sql(
//      s"""
//        |select 	a.imei
//        |from
//        |	(
//        |		select	imei,
//        |				count(1) as cn
//        |		from 	ods.ods_media_req
//        |		where etl_date = '$etlDate'
//        |		and 	etl_hour = '$etlHour'
//        |		and		imei is not null
//        |		and		imei <> ''
//        |		and		imei <> '000000000000000'
//        |   and     length(imei) = 15
//        |		group 	by imei
//        |	) a
//        |where 	a.cn >= 300
//        |order 	by a.cn desc
//        |""".stripMargin
    s"""
         |select
         |imei
         |from
         |(
         |select
         |imei,cnt
         |from
         |    (
         |        select
         |        imei,count(1) as cnt
         |        from     ods.ods_media_req
         |        where etl_date = '$etlDate'
         |        and     etl_hour = '$etlHour'
         |        and        imei <> ''
         |        and        imei <> '000000000000000'
         |        and     length(imei) = 15
         |        group     by imei
         |
         |    ) a
         |where cnt >= 300
         |order by cnt desc
         |) t
         |""".stripMargin
    )
    RedisDAO.writeToRedis(mediaReqIMeiDF:DataFrame,"imei":String)

  val mediaReqAndroidIdDF = spark.sql(
    s"""
      |select
      | android_id
      |from
      |(
      |select a.android_id,a.cn
      |from
      |	(
      |		select	android_id,
      |				count(1) as cn
      |		from 	ods.ods_media_req
      |		where etl_date = '$etlDate'
      |		and 	etl_hour = '$etlHour'
      |		and		android_id is not null
      |		and		android_id <> ''
      |   and   android_id <> '00000000-0000-0000-0000-000000000000'
      |		group by android_id
      |	) a
      |where 	a.cn >= 300
      |order 	by a.cn desc
      |) t
      |""".stripMargin)
    RedisDAO.writeToRedis(mediaReqAndroidIdDF:DataFrame,"android_id":String)

    val mediaReqIdFaDF = spark.sql(
//      s"""
//        |select 	a.idfa
//        |from
//        |	(
//        |		select	idfa,
//        |				count(1) as cn
//        |		from 	ods.ods_media_req
//        |		where etl_date = '$etlDate'
//        |		and 	etl_hour = '$etlHour'
//        |		and		idfa is not null
//        |		and		idfa <> ''
//        |		and 	idfa <> '00000000-0000-0000-0000-000000000000'
//        |		group by idfa
//        |	) a
//        |where 	a.cn >= 300
//        |order 	by a.cn desc
//        |""".stripMargin
      s"""
         |select
         |idfa
         |from
         |(
         |select     a.idfa,a.cn
         |from
         |    (
         |        select    idfa,
         |                count(1) as cn
         |        from     ods.ods_media_req
         |        where etl_date = '$etlDate'
         |        and     etl_hour = '$etlHour'
         |        and        idfa is not null
         |        and        idfa <> ''
         |        and     idfa <> '00000000-0000-0000-0000-000000000000'
         |        group by idfa
         |    ) a
         |where     a.cn >= 300
         |order     by a.cn desc
         |) t
         |""".stripMargin

              )
    RedisDAO.writeToRedis(mediaReqIdFaDF:DataFrame,"idfa":String)

  }

  /**
   * 20201110 反作弊新规则
   *
   * 1 401日志中也加入和101一样的检查
   * 2 101和401日志中，1小时出现次数大于50次的imei、anid、idfa、ip，存redis，过期时间2小时
   * 3 101和401日志中，1小时出现次数大于500次的ip段，存redis，过期时间2小时
   *
   * @param spark
   * @param etlDate
   * @param etlHour
   */
  def cheapAnalysis2(spark:SparkSession ,etlDate:String,etlHour:String) = {
    //ip转16进制
    //举例：117.157.198
    //转16进制并拼接：759dc6
    spark.udf.register("to_hex",(str:String) => {
      if((str == null) || (str isEmpty)){
         null
      }else{
        val arr = str.split("\\.")
        if(arr.length != 3){
           null
        }else{
           arr(0).toInt.toHexString+arr(1).toInt.toHexString+arr(2).toInt.toHexString
        }
      }
    })

   implicit val cheapUnionDF = spark.sql (
      s"""
        |select
        |device_type,v,max(cnt) as cnt
        |from
        |(
        |select
        |device_type,v,log_type,
        |count(1) as cnt
        |
        |from
        |(
        |    -- 101日志
        |    select
        |       '101' as log_type,
        |        k device_type,
        |        v
        |        from
        |        (
        |        select *
        |    from
        |        ods.ods_media_req
        |    where
        |        etl_date='$etlDate'  and etl_hour='$etlHour'
        |        ) t
        |    lateral view explode(map('imei',imei,'idfa',idfa,'ip',ip,'anid',android_id,'ip3',to_hex(regexp_extract(ip,'([0-9]+\\\\.\\\\d+\\\\.\\\\d+)')) )) m as k,v
        |    where (v is not null and v !='' and v!='\\\\N' and (not v regexp '^[0-]*$$') and v !='没有权限' and v != 'unknown')
        |        or ( k = 'ip' and ip regexp '((2(5[0-5]|[0-4]\\\\d))|[0-1]?\\\\d{1,2})(\\\\.((2(5[0-5]|[0-4]\\\\d))|[0-1]?\\\\d{1,2})){3}' )
        |
        |    union all
        |    -- 401日志
        |    select
        |       '401' as log_type,
        |        k device_type,
        |        v
        |        from
        |        (
        |        select *
        |    from
        |        ods.ods_media_req_filter
        |    where
        |        etl_date='$etlDate'  and etl_hour='$etlHour'
        |        ) t
        |    lateral view explode(map('imei',imei,'idfa',idfa,'ip',ip,'anid',android_id,'ip3',to_hex(regexp_extract(ip,'([0-9]+\\\\.\\\\d+\\\\.\\\\d+)') ))) m as k,v
        |    where (v is not null and v !='' and v!='\\\\N' and (not v regexp '^[0-]*$$') and v !='没有权限' and v != 'unknown')
        |        or ( k = 'ip' and ip regexp '((2(5[0-5]|[0-4]\\\\d))|[0-1]?\\\\d{1,2})(\\\\.((2(5[0-5]|[0-4]\\\\d))|[0-1]?\\\\d{1,2})){3}' )
        |) t
        |group by
        |device_type,v,log_type
        |having
        |count(1) >
        |case
        | when device_type in ('ip','idfa','anid','imei') then 50
        | when device_type ='ip3' then 500
        |end
        |
        |) t
        |group by device_type,v
        |""".stripMargin )

    cheapUnionDF.cache()
    val cheapUnionCount = cheapUnionDF.count()  //要保证2000条pipline提交一次
    implicit val numPartition = ( cheapUnionCount / 2000 + 1 ).toInt

    val redisDB = 0  // 塞入到redis select(0)

    cheapUnionDF.createOrReplaceTempView("cheapUnionDF")

    //导入到2个redis
    RedisDAO.writeToRedis2("172.16.189.196", 6379,0)

    RedisDAO.writeToRedis2("172.16.189.215", 6379,0)

    //输出文件到hdfs,输出到北京redis
    spark.sql("insert overwrite directory '/home/ssp/cheap_data' ROW FORMAT DELIMITED FIELDS TERMINATED by ',' select device_type,v  from cheapUnionDF")

  }
}
