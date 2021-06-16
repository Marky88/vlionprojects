package com.advlion.www.task.customer_task

import java.util

import com.advlion.www.util.PropertiesUtil
import com.mongodb.MongoClient
import com.mongodb.client.model.{Filters, UpdateOneModel, UpdateOptions, WriteModel}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.bson.Document

/**
 * @description:
 * @author: malichun
 * @time: 2021/4/2/0002 10:16
 * 20210402 两台服务器的日志处理,导入mongodb
 *  http://wiki.vlion.cn/pages/viewpage.action?pageId=36439396
 *
 *  服务器处理:
 *  /home/spark/ssp_customer/3_rta_log_process
 *
 *
 */
object SspRtaMongodb {
    // mongodb的配置
    val mongoProperties = PropertiesUtil.load("mongodb.properties")
    private val mongoHost = mongoProperties.getProperty("host")
    private val mongoPort = mongoProperties.getProperty("port").toInt
    val mongoDatabaseCrowd = "crowd"
    val mongoCollectionRta = "test1"

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession
            .builder()
            //     .master("local[*]")
            .appName("SspImeiIntersection")
            .config("hive.metastore.uris","trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir","/user/hive/warehouse")
            .config("spark.debug.maxToStringFields","100")
            .enableHiveSupport()
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        val sc = spark.sparkContext
        val etlDate = args(0)
        val etlHour = args(1)

        try{

            val unionDf = spark.sql(
                s"""
                   |
                   |select
                   |logtype,
                   |max(device_id) as device_id, -- 防止deviceId没有东西
                   |device_id_md5,
                   |device_type,
                   |max(if(r_n = 1 ,time,null)) as time,
                   |max(if(r_n = 1, result,null)) as result
                   |from
                   |(
                   |    SELECT
                   |    logtype,
                   |    device_id,
                   |    device_id_md5,
                   |    device_type,
                   |    result,
                   |    time,
                   |    row_number() over(partition by device_id_md5,device_type order by time desc) as r_n
                   |    from 
                   |    (
                   |        select
                   |            logtype,
                   |            if(length(device_id) = 32,null, device_id) as device_id, -- 明文
                   |            if(length(device_id) = 32,device_id, md5(device_id)) as device_id_md5,  --md5后的
                   |            device_type,
                   |            result,
                   |            time
                   |        from 
                   |            ods.ods_ssp_rta_alipay_resp
                   |        where etl_date='${etlDate}' and etl_hour='${etlHour}' and (device_id is not null and device_id != '') 
                   |    ) t   
                   |) t
                   |group by logtype,device_id_md5,device_type
                   |
                   |
                   |union all
                   |
                   |select
                   |logtype,
                   |max(device_id) as device_id, -- 防止deviceId没有东西
                   |device_id_md5,
                   |device_type,
                   |max(if(r_n = 1 ,time,null)) as time,
                   |max(if(r_n = 1, result,null)) as result
                   |from
                   |(
                   |    SELECT
                   |    logtype,
                   |    device_id,
                   |    device_id_md5,
                   |    device_type,
                   |    result,
                   |    time,
                   |    row_number() over(partition by device_id_md5,device_type order by time desc) as r_n
                   |    from 
                   |    (
                   |        select
                   |            logtype,
                   |            if(length(device_id) = 32,null, device_id) as device_id, -- 明文
                   |            if(length(device_id) = 32,device_id, md5(device_id)) as device_id_md5,  --md5后的
                   |            device_type,
                   |            result,
                   |            time
                   |        from 
                   |            ods.ods_ssp_rta_dhh_resp
                   |        where etl_date='${etlDate}' and etl_hour='${etlHour}' and (device_id is not null and device_id != '') 
                   |    ) t   
                   |) t
                   |group by logtype,device_id_md5,device_type
                   |
                   |""".stripMargin)

            unionDf.rdd.map(row =>{
                (row.getAs[String]("logtype"),
                    row.getAs[String]("device_id"),
                    row.getAs[String]("device_id_md5"),
                    row.getAs[String]("device_type"),
                    row.getAs[String]("time"),
                    row.getAs[String]("result")
                )
            }).repartition(500)
                .foreachPartition(uploadToMongoDB)

        }catch {
            case e:Exception => e.printStackTrace()
        }finally {
            spark.close()
            sc.stop()
        }
    }


    /**
     *
     *  //单个task导入到mongodb
     * 全量人群库字段,导入mongo,采用追加更新方式
     *
     *
     * db.comment.updateOne(
     * {_id:"1234567"},
     * {
     * $set:{
     *      "imei":"imei123",
     *      "imei_md5":"imei_md5123",
     *      "alipay.clk_time":1616571240,
     *      "alipay.ocpx_time":1616571240,
     *      "alipay.ocpx_tag":4
     * },
     * $inc:{
     *      "alipay.imp_count":NumberInt(20),
     *      "alipay.clk_count":NumberInt(20)
     * }
     * },
     * {
     *      upsert:true
     * }
     * )
     *
     ***/
    def uploadToMongoDB(items: Iterator[(String,String,String,String,String,String)]): Unit ={
        val mongoClient = new MongoClient(mongoHost, mongoPort)
        val collectionRta = mongoClient.getDatabase(mongoDatabaseCrowd).getCollection(mongoCollectionRta)

        val upsertBatchSize = 5000
        var upsertIndexCount = 0
        val list = new util.ArrayList[WriteModel[Document]](upsertBatchSize)

        items.foreach(item => { //针对一条记录
            upsertIndexCount += 1
            val outDocument = new Document()
            val setDocument = new Document()
            outDocument.append("$set", setDocument)
            //设备类型:
            //alipay
            // 1imei 2idfa 3oaid 4其他

            //dhh
            // 1imei 2imei_md5 3idfa 4oaid 5其他 6idfa_md5 7oaid_md5
            item match {
                case ("ali",device_id,device_id_md5,"1",time,result) => //alipay  imei
                    setDocument.append("imei_md5",device_id_md5)
                    if(device_id!=null && device_id != "") setDocument.append("imei",device_id)
                    setDocument.append("alipay.rta_time",time)
                    setDocument.append("alipay.rta_tag",result)

                    val updateBson = Filters.eq("imei_md5", device_id_md5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))

                    list.add(updateModel)

                case ("ali",device_id,device_id_md5,"2",time,result) => //alipay  idfa
                    setDocument.append("idfa_md5",device_id_md5)
                    if(device_id!=null && device_id != "") setDocument.append("idfa",device_id)
                    setDocument.append("alipay.rta_time",time)
                    setDocument.append("alipay.rta_tag",result)

                    val updateBson = Filters.eq("idfa_md5", device_id_md5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))

                    list.add(updateModel)

                case ("ali",device_id,device_id_md5,"3",time,result) => //alipay  oaid
                    setDocument.append("oaid_md5",device_id_md5)
                    if(device_id != null && device_id != "") setDocument.append("oaid",device_id)
                    setDocument.append("alipay.rta_time",time)
                    setDocument.append("alipay.rta_tag",result)

                    val updateBson = Filters.eq("oaid_md5", device_id_md5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))

                    list.add(updateModel)

                case ("dhh",device_id,device_id_md5,device_type,time,result) if device_type == "1" || device_type == "2" => //dhh imei
                    setDocument.append("imei_md5",device_id_md5)
                    if(device_id!=null && device_id != "") setDocument.append("imei",device_id)
                    setDocument.append("dhh.rta_time",time)
                    setDocument.append("dhh.rta_tag",result)

                    val updateBson = Filters.eq("imei_md5", device_id_md5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))

                    list.add(updateModel)
                case ("dhh",device_id,device_id_md5,device_type,time,result) if device_type == "3" || device_type == "6" => //dhh idfa
                    setDocument.append("idfa_md5",device_id_md5)
                    if(device_id!=null && device_id != "") setDocument.append("idfa",device_id)
                    setDocument.append("dhh.rta_time",time)
                    setDocument.append("dhh.rta_tag",result)

                    val updateBson = Filters.eq("idfa_md5", device_id_md5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))

                    list.add(updateModel)
                case ("dhh",device_id,device_id_md5,device_type ,time,result)  if device_type == "4" || device_type == "7"=> //dhh oaid
                    setDocument.append("oaid_md5",device_id_md5)
                    if(device_id != null && device_id != "") setDocument.append("oaid",device_id)
                    setDocument.append("dhh.rta_time",time)
                    setDocument.append("dhh.rta_tag",result)

                    val updateBson = Filters.eq("oaid_md5", device_id_md5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))

                    list.add(updateModel)

                case _ =>
            }

            if (upsertIndexCount % upsertBatchSize == 0) {

                collectionRta.bulkWrite(list)
                list.clear()
            }

        }) //foreach的括号



        mongoClient.close()
    }

}
