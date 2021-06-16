package com.advlion.www.analysis

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import java.util
import java.util.Date

import com.advlion.www.jdbc.MySQL
import com.advlion.www.util.ParseJSONUtil
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.spark.Partitioner
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

object OfflineStatistics {
  val url: String = MySQL.url
  val user: String  = MySQL.user
  val password: String  = MySQL.password
  val driver: String  = MySQL.driver
  val resultTable = "stat_day"
  val dauTable = "ssp_dau_tmp"
  val creativeTable="stat_channel_creative"
  val creativeTableNew = "stat_channel_creative_new" //活动新的统计20200618
  val creativeImgTableNewTemp ="creative_img_temp"  //mysql creative_img_temp
  val statMediaPkgTemp = "stat_media_pkg_temp" //  20200624包名的count

  def summary(spark: SparkSession,args: Array[String]): Unit ={
    val etlDate = args(0).toString
    val etlHour = args(1).toString
    val etlTime = etlDate + " " + etlHour + ":00:00"
    val resultDF = spark.sql(
      s"""
        |select
        |media_id,
        |channel_id,
        |adslocation_id,
        |dev_id,
        |joint_id,
        |max(deduct) as deduct,
        |time,
        |hour,
        |max(valid_uv) as valid_uv,
        |max(valid_pv) as valid_pv,
        |max(sdk_uv)  as sdk_uv,
        |max(sdk_pv)  as sdk_pv,
        |max(filter_req)  as filter_req,
        |max(adv_back)  as adv_back,
        |max(def_show)  as def_show,
        |max(def_click)  as def_click,
        |max(show)  as show,
        |max(click)  as click,
        |max(dpnum)   as dpnum,
        |max(clkdpnum)   as clkdpnum,
        |max(send_dsp_res)   as send_dsp_res,
        |is_upload,
        |del,
        |max(channel_uv)   as channel_uv,
        |max(channel_pv)   as channel_pv,
        |am_id,
        |max(down_start)   as down_start,
        |max(down_complate)   as down_complate,
        |max(install_start) as install_start,
        |max(install_complate) as install_complate,
        |max(app_start) as app_start,
        |max(v_cache_done) as v_cache_done,
        |max(v_play_start) as v_play_start,
        |max(v_play_done) as v_play_done,
        |max(rjcost) as rjcost
        |from
        |(
        |    select case when t1.app_id = '' then null else t1.app_id end as media_id,
        |            case when nvl(t1.dsp_id,-1)='' then -1 else nvl(t1.dsp_id,-1) end as channel_id,
        |            t1.adsolt_id as adslocation_id,
        |            t2.user_id as dev_id,
        |            t3.joint_id,
        |            nvl(t3.deduct,0) as deduct,
        |            '$etlDate' as time,
        |            '$etlTime' as hour,
        |            t1.valid_uv,
        |            t1.valid_pv,
        |            t1.sdk_uv,
        |            t1.sdk_pv,
        |           t1.filter_req,
        |           t1.adv_back,
        |           t1.def_show,
        |           t1.def_click,
        |           t1.show,
        |           t1.click,
        |           t1.dpnum,
        |           t1.clkdpnum,
        |           t1.send_dsp_res,
        |            'n' is_upload,
        |            'no' del,
        |            t1.channel_uv,
        |            t1.channel_pv,
        |            nvl(t4.user_id,-1) am_id,
        |      t1.down_start ,
        |      t1.down_complate ,
        |      t1.install_start ,
        |      t1.install_complate,
        |      t1.app_start ,
        |      t1.v_cache_done ,
        |      t1.v_play_start ,
        |      t1.v_play_done,
        |      t1.rjcost
        |
        |    from summary t1
        |    left join  media t2
        |    on t1.app_id = t2.id
        |    left join  adsLocation t3
        |    on t1.adsolt_id = t3.id
        |    left join  userMediaMap t4
        |    on t1.app_id = t4.id
        |)
        |group by
        |time,dev_id,media_id,channel_id,adslocation_id,is_upload,del,hour,joint_id,am_id
        |""".stripMargin)
//    println("往mysql塞入的条数据"+resultDF.count())

//    resultDF.rdd.saveAsTextFile("hdfs://www.bigdata02.com:8020/user/test/aa.txt")

    resultDF.coalesce(5)
      .write.mode("append")
      .format("jdbc")
      .option("url",url)
      .option("driver",driver)
      .option("user",user)
      .option("password",password)
      .option("dbtable",resultTable)
      .save()
  }

  /**
   * 媒体请求用户日活统计
   */
  def dauStatistics(spark: SparkSession): Unit ={
    val dauResultDF = spark.sql(
      """
        |select t1.time,
        |		t1.app_id as media_id,
        |		t1.dsp_id as channel_id,
        |		t1.adsolt_id as adslocation_id,
        |  	t2.user_id as dev_id,
        |		t3.joint_id,
        |		nvl(t4.user_id,-1) am_id,
        |		t1.valid_uv,
        |		t1.valid_pv,
        |		t1.android_sdk_uv,
        |		t1.ios_sdk_uv,
        |		t1.android_sdk_pv,
        |		t1.ios_sdk_pv
        |from dau t1
        |left join  media t2
        |on t1.app_id = t2.id
        |left join  adsLocation t3
        |on t1.adsolt_id = t3.id
        |left join  userMediaMap t4
        |on t1.app_id = t4.id
        |""".stripMargin)

    dauResultDF.coalesce(5).write.mode("overwrite")
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("driver",driver)
      .option("dbtable",dauTable)
      .save()
  }

  /**
   * 创意日志数据统计
   * @param spark
   * @param args
   */
  def dspCreativeStatistics(spark: SparkSession,args: Array[String]): Unit ={
    val etlDate = args(0).toString
    val etlHour = args(1).toString
    val etlTime = etlDate + " " + etlHour + ":00:00"

    //    val countDF = spark.sql(
    //     s"""
    //        |select '$etlDate' as time,
    //        |		    '$etlTime' as hour,
    //        |       unique,
    //        |		    count(1) as count
    //        |from 	ods.ods_dsp_resp_creative
    //        |where	etl_date = '$etlDate'
    //        |and 	  etl_hour = '$etlHour'
    //        |group 	by unique
    //        |""".stripMargin)
    //    countDF.write.mode("append")
    //      .format("jdbc")
    //      .option("url",url)
    //      .option("user",user)
    //      .option("password",password)
    //      .option("driver",driver)
    //      .option("dbtable",creativeTable)
    //      .save()
    //
    //    val creativeDF = spark.sql(
    //      s"""
    //        |select	'$etlDate' as time,
    //        |		'$etlTime' as hour,
    //        |		creative_type as adslocation_type_id,
    //        |		creative_json,
    //        |		channel_url,
    //        |		dsp_id as channel_id,
    //        |		res_type,
    //        |		media_id,
    //        |		unique
    //        |from 	ods.ods_dsp_resp_creative
    //        |where	etl_date = '$etlDate'
    //        |and 	  etl_hour = '$etlHour'
    //        |""".stripMargin)
    //
    //    creativeDF.write.mode("overwrite")
    //      .format("jdbc")
    //      .option("url",url)
    //      .option("user",user)
    //      .option("password",password)
    //      .option("driver",driver)
    //      .option("dbtable",creativeTmpTable)
    //      .save()

    val creativeDF = spark.sql(
      s"""
         |select	'$etlDate' as time,
         |		'$etlTime' as hour,
         |		creative_type as adslocation_type_id,
         |		creative_json,
         |		channel_url,
         |		dsp_id as channel_id
         |from 	ods.ods_dsp_resp_creative
         |where	etl_date = '$etlDate'
         |and 	  etl_hour = '$etlHour'
         |and creative_type regexp '^\\\\d+$$'
         |""".stripMargin)

    creativeDF.coalesce(5).write.mode("append")
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("driver",driver)
      .option("dbtable",creativeTable)
      .save()

  }

  /**
   * 创意日志数据统计2
   * @param spark
   * @param args
   */
  def dspCreativeStatisticsNew(spark: SparkSession,args: Array[String]): Unit ={
    val etlDate = args(0)
    val etlHour = args(1)
    val etlTime = etlDate + " " + etlHour + ":00:00"

    val creativeDF = spark.sql(
      s"""
         |
         |select
         |  '$etlDate' as time,
         |  '$etlTime' as hour,
         |  max(creative_type) as adslocation_type_id,
         |  max(creative_json) as creative_json,
         |  max(channel_url) as channel_url,
         |  max(dsp_id) as channel_id,
         |  max(res_type) as res_type,
         |  max(media_id) as media_id,
         |  count(1) as count,
         |  unique
         |from 	ods.ods_dsp_resp_creative
         |where	etl_date = '$etlDate'
         |and 	  etl_hour = '$etlHour'
         |and creative_type regexp '^\\\\d+$$'
         |group by `unique`
         |
         |""".stripMargin)

    creativeDF.coalesce(5).write.mode("append")
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password",password)
      .option("driver",driver)
      .option("dbtable",creativeTableNew)
      .save()

  }

  //20200701处理表creative_img,提取图片url到mysql:creative_img
  def dspCreativeStatisticsImg(spark:SparkSession,args:Array[String]):Unit={
    val etlDate = args(0)
    val etlHour = args(1)
    val etlTime = etlDate + " " + etlHour + ":00:00"

    spark.udf.register("get_image_url",ParseJSONUtil.parseLogReturnImgUrl _)
    val creativeImgDF =spark.sql(
      s"""
         |
         |select
         | img_url,channel_id,img_url_md5,count(1) as count,adslocation_id
         |from
         |(
         |select
         |get_image_url(creative_json,if(creative_type=3,1,0)) as img_url,
         |dsp_id as channel_id,
         |md5(get_image_url(creative_json,if(creative_type=3,1,0))) as img_url_md5,
         |adsolt_id as adslocation_id
         |from 	ods.ods_dsp_resp_creative
         |where	etl_date = '$etlDate'
         |and 	  etl_hour = '$etlHour'
         |) t
         |group by img_url,channel_id,img_url_md5,adslocation_id
         |
         |""".stripMargin)

    creativeImgDF.coalesce(5).write.mode("overwrite")
        .format("jdbc")
        .option("url",url)
        .option("user",user)
        .option("password",password)
        .option("driver",driver)
        .option("dbtable",creativeImgTableNewTemp)
        .save()

  }



  /**
   * 20200624统计ssp_imp 的 media_pkg ,
   * 每小时统计当天的count,数据导入到临时表 stat_media_pkg_temp
   * @param spark
   * @param args
   */
  def summaryMediaPkg(spark:SparkSession,args: Array[String]): Unit ={
    val etlDate = args(0)
    val etlHour = args(1)
    val etlTime = etlDate + " " + etlHour + ":00:00"
    val currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    val mediaPkgDF=spark.sql(
      s"""
         |select
         |app_id as media_id,
         |'${etlDate}' as time,
         |http_haad as pkg,
         |'${currentTime}' as update_time,
         |count(1) as count
         |from
         |  ods.ods_ssp_imp
         |where etl_date = '$etlDate'
         |group by app_id,http_haad
         |
         |""".stripMargin)
    mediaPkgDF.persist(StorageLevel.MEMORY_AND_DISK)

    mediaPkgDF.coalesce(5).write.mode("overwrite")
        .format("jdbc")
        .option("url",url)
        .option("user",user)
        .option("password",password)
        .option("driver",driver)
        .option("dbtable",statMediaPkgTemp)
        .save()

  }


    /**
     * 20201215
     * 步骤：
     * 1 每三天一个定时任务，提取idfa，oaid，imei，分别去重
     * 2 多线程请求接口，
     * 内网地址：http://172.16.209.166:8567/alipay_rta
     * post方法，一次最多5个原文设备id的md5
     * {
     * // 大写 IMEI，IDFA，OAID
     * "device_type": "IMEI",
     * "device_ids": [
     * "9969ef1d73289350111b2755a347ceb4",
     * "9123124453289350111b2755a347ceb4"
     * ]
     * }
     * 请求中的设备id，如果不在返回中，则不用管
     * 如果在返回中，取出设备id和对应的值，如下面的 L00002
     * 返回值示例
     * {"status":0,"data":[{"device_id":"9944ef1d73289350111b2755a347ceb4","device_label":"L00002"},{"device_id":"9459ef1d73289350111b2755a347ceb4","device_label":"L00002"},{"device_id":"9129ef1d73289350111b2755a347ceb4","device_label":"L00002"},{"device_id":"9239ef1d73289350111b2755a347ceb4","device_label":"L00002"},{"device_id":"9969ef1d73289350111b2755a347ceb4","device_label":"L00016"}]}
     * 3 将设备id和对应的值，存入文件，并存入ssdb
     * 4 每天再定时3小时取出曝光日志中渠道id=79的设备id，请求接口，把没有返回值的id，从ssdb中删除
     *
     *
     * 最终生成文件,每3小时
     * @param spark
     * @param etlDate
     * @param etlHour
     */
    def mediaReqThreeHourGenerate(spark: SparkSession, etlDate: String, etlHour: String): Unit = {
        import spark.implicits._
//        val formatter3 = DateTimeFormatter.ofPattern("yyyy-MM-dd")

        //前3天
//        val beginDay = LocalDate.parse(etlDate, formatter3).minusDays(3).toString


        val beforeHour: String = etlHour match{
            case "23" => "21"
            case "20" => "18"
            case "17" => "15"
            case "14" => "12"
            case "11" => "09"
            case "08" => "06"
            case "05" => "03"
            case "02" => "00"
        }

        val mediaReqDF = spark.sql(
            s"""
               |select
               |distinct k,md5(v) as v
               |from
               |(
               |  select
               |  idfa,
               |  oaid,
               |  imei
               |  from
               |  ods.ods_media_req where  etl_date = '${etlDate}' and ( etl_hour <='${etlHour}' and etl_hour >= '${beforeHour}' )
               |  and app_id  not in ('30910','21013','21012','30120','30121','30135','30136','20898','20896')  -- 明确是假量
               |) t
               |lateral view explode(map('IDFA',idfa,'IMEI',imei,'OAID',oaid)) m as k,v
               |where v not like '%00000000000000%'
               |""".stripMargin)

        //删除hdfs目录
        val outputDir = "/data/spark/ssp/data/output/media_req_3_hour"
        FileSystem.get(new Configuration()).delete(new Path(outputDir),true)
//        val conf1 = new Configuration()
//        val fs = FileSystem.get(conf1)
//        fs.delete(new Path(outputDir),true)
//        fs.mkdirs(new Path(outputDir))
        //生成文件
        mediaReqDF
            .rdd
            .map(row => ((row.getAs[String]("k"), Random.nextInt(500)), row.getAs[String]("v")))
            .groupByKey()
            .foreach { case ((k: String, random: Int), iter: Iterable[String]) =>

                val configuration = new Configuration()
                val fs = FileSystem.get(configuration)

                // 输出到当前目录
                val outputStream = fs.create(new Path(s"${outputDir}/${k}_${random}.txt"))
                val bufferWriter = new BufferedWriter(new OutputStreamWriter(outputStream))
                iter.foreach(line => {
                    if (line != null) {
                        bufferWriter.write(line)
                        bufferWriter.newLine()
                        bufferWriter.flush()
                    }
                })
                bufferWriter.close()
                outputStream.close()
                fs.close()
            }



//        val value = mediaReqDF
//            .rdd
//            .map(r => ((r.getAs[String]("k"), Random.nextInt(30)), r.getAs[String]("v")))
//    val rdd = spark.sparkContext.textFile("/tmp/test/20201215_media_req").map( line =>{
//                val arr = line.split(",")
//                ((arr(0),Random.nextInt(800)),arr(1))
//            })
//            .groupByKey()
//            .flatMap { case ((k: String, random: Int), iter: Iterable[String]) =>
//                val tagFront: String = k match {
//                    case "IMEI" => "hv:m:"
//                    case "OAID" => "hv:o:"
//                    case "IDFA" => "hv:i:"
//                }
//                println(s"process type : ${k}_${random} current total count:" + iter.size)
//                val return_ = ListBuffer[String]()
//
//                val cm = new PoolingHttpClientConnectionManager()
//                cm.setMaxTotal(100)
//                cm.setDefaultMaxPerRoute(20)
//                cm.setDefaultMaxPerRoute(50)
//                val httpClient: CloseableHttpClient = HttpClients.custom().setConnectionManager(cm).build()
//                val url = "http://172.16.209.166:8567/alipay_rta"
//                val httpPost = new HttpPost(url)
//                val requestConfig: RequestConfig = RequestConfig.custom().setConnectTimeout(30000).setConnectionRequestTimeout(30000).setSocketTimeout(30000).build()
//                httpPost.setConfig(requestConfig)
//                httpPost.setConfig(requestConfig)
//                httpPost.addHeader("Content-type", "application/json; charset=utf-8")
//                httpPost.setHeader("Accept", "application/json")
//                val NL = System.getProperty("line.separator")
//                val iterator: Iterator[String] = iter.iterator
//                val deviceIds = new util.ArrayList[String]()
//                val jsonObject = new JSONObject()
//                jsonObject.put("device_type", k)
//                jsonObject.put("device_ids", deviceIds)
//
//                var whileFlag = 0
//                var calCount = 0
//                while (iterator.hasNext) {
//                    calCount += 1
//                    if(calCount % 5000 == 0){
//                        println(s"====job ${k}_${random} === ${LocalDateTime.now()}  has complete ${calCount} cnt data ====")
//                    }
//                    if (deviceIds.size != 5) {
//                        deviceIds.add(iterator.next())
//                    } else {
//                        while (whileFlag <= 50) {
//                            val jsonString: String = jsonObject.toJSONString
//                            httpPost.setEntity(new StringEntity(jsonString, Charset.forName("UTF-8")))
//                            val response = httpClient.execute(httpPost)
//                            val in = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
//                            val sb = new StringBuilder()
//                            var line = ""
//                            var flag: Boolean = true
//                            while (flag) {
//                                line = in.readLine()
//                                if (line != null && line != "") {
//                                    sb.append(line + NL)
//                                } else {
//                                    flag = false
//                                }
//                            }
//                            in.close()
//                            val result = sb.toString
//                            deviceIds.clear()
//                            response.close()
//
//                            val jsonObject1 = JSON.parseObject(result)
//                            val status = jsonObject1.getIntValue("status")
//                            if (status != 0) { //请求失败
//                                whileFlag += 1
//                                if (whileFlag == 51) {
//                                    throw new Exception("请求50次还是报错,失败,请求参数:" + jsonString)
//                                }
//                            } else { //请求成功
//                                whileFlag = 101
//                                val data = jsonObject1.getJSONArray("data")
//                                if (data != null) {
//                                    for (i <- 0 until data.size()) {
//                                        val nObject: JSONObject = data.getJSONObject(i)
//                                        val deviceId = nObject.getString("device_id")
//                                        val deviceLabel = nObject.getString("device_label")
//
//                                        return_.append(tagFront + deviceId + "_" + deviceLabel)
//                                    }
//                                }
//                            }
//                        } //多次请求的括号
//                        whileFlag = 0
//                    } // if的括号
//                } //while循环每次请求
//                if (deviceIds.size() != 0) {
//                    while (whileFlag <= 50) {
//                        val jsonString: String = jsonObject.toJSONString
//                        httpPost.setEntity(new StringEntity(jsonString, Charset.forName("UTF-8")))
//                        val response = httpClient.execute(httpPost)
//                        val in = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
//                        val sb = new StringBuilder()
//                        var line = ""
//                        var flag: Boolean = true
//                        while (flag) {
//                            line = in.readLine()
//                            if (line != null && line != "") {
//                                sb.append(line + NL)
//                            } else {
//                                flag = false
//                            }
//                        }
//                        in.close()
//                        val result = sb.toString
//                        deviceIds.clear()
//                        response.close()
//
//                        val jsonObject1 = JSON.parseObject(result)
//                        val status = jsonObject1.getIntValue("status")
//                        if (status != 0) { //请求失败
//                            whileFlag += 1
//                            if (whileFlag == 51) {
//                                throw new Exception("请求50次还是报错,失败,请求参数:" + jsonString)
//                            }
//                        } else { //请求成功
//                            whileFlag = 101
//                            val data = jsonObject1.getJSONArray("data")
//                            if (data != null) {
//                                for (i <- 0 until data.size()) {
//                                    val nObject: JSONObject = data.getJSONObject(i)
//                                    val deviceId = nObject.getString("device_id")
//                                    val deviceLabel = nObject.getString("device_label")
//
//                                    return_.append(tagFront + deviceId + "_" + deviceLabel)
//                                }
//                            }
//                        }
//                    } //一次数据多次请求的while
//                } // 看是不是刚好没有余的
//                println("")
//                return_.toList
//            }


//        rdd.saveAsTextFile("/tmp/test/20201215_media_req_out/9")
        /*
              doPost("IMEI",List("9969ef1d73289350111b2755a347ceb4","9123124453289350111b275s5a347ceb4","9944ef1d73289350111b2755a347ceb4"
                        ,"9459ef1d73289350111b2755a347ceb4","9969ef1d73289350111b2755a347sceb4","9123124453289350111b275s5a347ceb42"))
        */
    }

    //同上,每小时处理一次媒体请求
    //每1小时再跑曝光日志dspid=79的，取出设备ID请求，如果没有label，则把对应的key里面的field从ssdb中删除: hdel(key, field)
    def impHourGenerate(spark: SparkSession, etlDate: String, etlHour: String): Unit ={

        val impDF = spark.sql(
            s"""
               |select
               |distinct k,md5(v) as v
               |from
               |(
               |    select
               |    *
               |    from
               |    ods.ods_ssp_imp
               |    where etl_date='${etlDate}' and etl_hour='${etlHour}'
               |    and dsp_id='79' and app_id  not in ('30910','21013','21012','30120','30121','30135','30136','20898','20896')
               |) t
               |lateral view explode(map('IDFA',idfa,'IMEI',imei,'OAID',oaid)) m as k,v
               |where v not like '%00000000000000%'
               |""".stripMargin)
        val outputDir = "/data/spark/ssp/data/output/ssp_imp_id"
        FileSystem.get(new Configuration()).delete(new Path(outputDir),true)

        impDF.rdd
            .map(r => (r.getAs[String]("k"),r.getAs[String]("v")))
            .groupByKey()
            .foreach { case (k, iter: Iterable[String]) =>

                val configuration = new Configuration()
                val fs = FileSystem.get(configuration)

                // 输出到当前目录
                val outputStream = fs.create(new Path(s"${outputDir}/${k}_.txt"))
                val bufferWriter = new BufferedWriter(new OutputStreamWriter(outputStream))
                iter.foreach(line => {
                    if (line != null) {
                        bufferWriter.write(line)
                        bufferWriter.newLine()
                        bufferWriter.flush()
                    }
                })
                bufferWriter.close()
                outputStream.close()
                fs.close()
            }


    }


}
