package com.advlion.www.analysis

import java.util

import com.advlion.www.jdbc.MySQLJdbc
import com.advlion.www.model.Crowd
import com.advlion.www.util.PropertiesUtil
import com.mongodb.{DBCollection, MongoClient}
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{Filters, InsertManyOptions, UpdateOneModel, UpdateOptions, WriteModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document
import org.bson.conversions.Bson
import java.security.MessageDigest
import java.sql.PreparedStatement

import com.advlion.www.load.ClickHouse
import org.apache.spark.sql.types.{BooleanType, DataType, DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType}
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource}

import scala.collection.JavaConverters._

object OfflineStatistics {
    private val url: String = MySQLJdbc.url
    private val user: String = MySQLJdbc.user
    private val password: String = MySQLJdbc.password
    private val driver: String = MySQLJdbc.driver

    // mongodb的配置
    val mongoProperties = PropertiesUtil.load("mongodb.properties")
    private val mongoHost = mongoProperties.getProperty("host")
    private val mongoPort = mongoProperties.getProperty("port").toInt
    val mongoDatabaseCrowd = "crowd"
    val mongoCollectionRta = "rta"

    //原始需求
    /*def summary(spark: SparkSession): Unit = {
        val resultTable = "stat_day_dsp_offline"
        val result = spark.sql(
            """
              |select	s.time,
              |		s.hour,
              |		s.plan_id,
              |		s.creative_id,
              |		s.adslot_id adslocation_id,
              |		s.channel_id,
              |		s.req,
              |		s.bid,
              |		s.show,
              |		s.click,
              |		s.cost,
              |		p.aduser_id,
              |		a.user_id dev_id,
              |		a.media_id,
              |		a.joint_id,
              |		nvl(c.creative_group_id,'-1') creative_group_id,
              |		case when u.user_oem_id is null  then '-1' else u.user_oem_id end as agent_id
              |from	summary s
              |left	join plan p
              |on		s.plan_id = p.id
              |left	join creative c
              |on		s.creative_id = c.id
              |left	join adsLocation a
              |on		s.adslot_id = a.id
              |left	join user u
              |on		s.plan_id = u.id
              |""".stripMargin)
        result.write.mode("append")
            .format("jdbc")
            .option("url", url)
            .option("user", user)
            .option("password", password)
            .option("driver", driver)
            .option("dbtable", resultTable)
            .save()
    }*/


    def summary(spark: SparkSession): Unit = {
        val resultTable = "mammu_stat_day_offline"

        val resultDF = spark.sql(
            """
              |  select
              |         s.etl_date,
              |  		s.etl_hour,
              |         s.etl_date_hour,
              |  		s.plan_id,
              |  		s.creative_id,
              |  		s.adslot_id adslocation_id,
              |         t.name as geo,
              |         s.adsolt_group_name as bundle,
              |  		s.channel_id,
              |  		s.req,
              |  		s.bid,
              |  		s.imp,
              |  		s.clk,
              |         s.ocpx,
              |  		cast(s.bid_real_cost as Decimal(18,10)) as bid_real_cost,
              |         cast(s.imp_real_cost as Decimal(18,10)) as imp_real_cost,
              |         s.alp_1,
              |         s.alp_2,
              |         s.alp_3,
              |         s.alp_4,
              |  		p.aduser_id as admaster_id,
              |  		a.user_id as dev_id,
              |  		a.media_id,
              |  		a.joint_id,
              |  		nvl(c.creative_group_id,'-1') creative_group_id,
              |  		case when u.user_oem_id is null  then '-1' else u.user_oem_id end as agent_id
              |  from	summary_tmp s
              |  left	join plan p
              |  on		s.plan_id = p.id
              |  left	join creative c
              |  on		s.creative_id = c.id
              |  left	join adsLocation a
              |  on		s.adslot_id = a.id
              |  left	join user u
              |  on		s.plan_id = u.id
              |  left   join city t
              |  on     s.geo = t.id
              |""".stripMargin)


       // resultDF.show(20)
      //  resultDF.rdd.saveAsTextFile("/tmp/test/qwe.csv")



        insertClickhouse(resultDF,ClickHouse.database,resultTable)



 //原始需求保存到mysql
/*
//保存到mysql
result.write.mode("append")
            .format("jdbc")
            .option("url", url)
            .option("user", user)
            .option("password", password)
            .option("driver", driver)
            .option("dbtable", resultTable)
            .save()*/

    }

    /**
     * 20210329 导入到mongodb
     */
    def uploadToMongoDB(spark: SparkSession, etlDate: String, etlHour: String): Unit = {
        val unionDF = spark.sql(
            s"""
               |SELECT
               |   device_type,
               |   device_value,
               |   model_type,
               |    max(imei) as imei,
               |    max(idfa) as idfa,
               |    max(ocpx_tag) as ocpx_tag,
               |    max(ocpx_time) as ocpx_time,
               |    sum(imp_count) as imp_count,
               |    max(clk_time) as clk_time,
               |    sum(clk_count) as clk_count
               |from
               |(
               | -- ocpx日志, 55
               |    SELECT
               |         device_type,
               |        device_value,
               |        model_type,
               |        imei,
               |        idfa,
               |        ocpx_tag,
               |        ocpx_time,
               |        imp_count,
               |        null as clk_time,
               |        clk_count
               |    from
               |    (
               |        select
               |            device_type,
               |            device_value,
               |            model_type,
               |            imei,
               |            idfa,
               |            ocpx_tag,
               |            ocpx_time,
               |             0 as imp_count,
               |            null as clk_time,
               |            0 as clk_count,
               |            ROW_NUMBER() over(partition by  device_type,device_value,model_type order by ocpx_time desc) as r_n
               |        from
               |        (
               |            select
               |                if(admaster_id = '21037','alipay','dhh') as model_type, -- 阿里/大航海
               |                if(length(imei) = 32,null,imei) as imei,
               |                if(length(imei) = 32,imei,md5(imei)) as imei_md5,
               |                if(length(idfa) = 32,null,idfa) as idfa,        --长度为36呀
               |                if(length(idfa) = 32,idfa,md5(idfa)) as idfa_md5,
               |                oaid,
               |                substring(ocpx_tag,5) as ocpx_tag, -- ocpx_类型     --substring(ocpx_tag,0,5) 截取前5位呀
               |                `time` as ocpx_time  -- 最后一次ocpx返回时间
               |            from
               |                ods.req_bid_imp_clk_55 t55
               |            where admaster_id in ('21037','20970')
               |            and etl_date='$etlDate' and etl_hour='${etlHour}'
               |        ) t
               |        lateral view explode(map("imei_md5",imei_md5,"idfa_md5",idfa_md5,"oaid",oaid)) tb55 as device_type,device_value
               |        where device_value is not null and device_value!=''
               |    ) t
               |       where r_n=1
               |
               |union all
               | -- 点击日志54
               |        SELECT
               |        device_type,
               |        device_value,
               |        model_type,
               |        max(imei) as imei,
               |        max(idfa) as idfa,
               |        null as ocpx_tag,
               |        null as ocpx_time,
               |        0 as imp_count,
               |        max(`time`) as clk_time,
               |        count(1) as clk_count
               |        from
               |        (
               |            SELECT
               |                if(admaster_id = '21037','alipay','dhh') as model_type, -- 阿里/大航海
               |                os, -- 操作系统,1安卓, 2 ios
               |                if(length(imei) = 32,null,imei) as imei,
               |                if(length(imei) = 32,imei,md5(imei)) as imei_md5,
               |                if(length(idfa) = 32,null,idfa) as idfa,
               |                if(length(idfa) = 32,idfa,md5(idfa)) as idfa_md5,
               |                oaid,
               |               `time`
               |            FROM
               |            ods.req_bid_imp_clk_54 t54
               |             where admaster_id in ('21037','20970')
               |            and etl_date='$etlDate' and etl_hour='${etlHour}'
               |        ) t
               |        lateral view explode(map("imei_md5",imei_md5,"idfa_md5",idfa_md5,"oaid",oaid)) tb54 as device_type,device_value
               |        where device_value is not null and device_value!=''
               |        group by device_type,device_value,model_type
               |
               |union all -- 曝光日志53
               |     SELECT
               |        device_type,
               |        device_value,
               |        model_type,
               |        max(imei) as imei,
               |        max(idfa) as idfa,
               |        null as ocpx_tag,
               |        null as ocpx_time,
               |        count(1) as imp_count,
               |        null as clk_time,
               |        0 as clk_count
               |    from
               |    (
               |        SELECT
               |            if(admaster_id = '21037','alipay','dhh') as model_type, -- 阿里/大航海
               |            if(length(imei) = 32,null,imei) as imei,
               |            if(length(imei) = 32,imei,md5(imei)) as imei_md5,
               |            if(length(idfa) = 32,null,idfa) as idfa,
               |            if(length(idfa) = 32,idfa,md5(idfa)) as idfa_md5,
               |            oaid,
               |           `time`
               |        FROM
               |        ods.req_bid_imp_clk_53 t53
               |         where admaster_id in ('21037','20970')
               |        and etl_date='$etlDate' and etl_hour='${etlHour}'
               |    ) t
               |      lateral view explode(map("imei_md5",imei_md5,"idfa_md5",idfa_md5,"oaid",oaid)) tb54 as device_type,device_value
               |        where device_value is not null and device_value!=''
               |        group by device_type,device_value,model_type
               |) total
               |group by
               |  device_type,device_value,model_type
               |""".stripMargin)
        val resRdd: RDD[Crowd] = unionDF.rdd.map(row => {
            Crowd(
                row.getAs[String]("device_type"),
                row.getAs[String]("device_value"),
                row.getAs[String]("model_type"), //alipay / dhh
                row.getAs[String]("imei"),
                row.getAs[String]("idfa"),
                row.getAs[String]("ocpx_tag"),
                if(row.getAs[String]("ocpx_time") == null || row.getAs[String]("ocpx_time") == "" ) 0L else row.getAs[String]("ocpx_time") toLong,
                row.getAs[Long]("imp_count") toInt,
                if(row.getAs[String]("clk_time") ==null || row.getAs[String]("clk_time") == "" ) 0L else row.getAs[String]("clk_time") toLong,
                row.getAs[Long]("clk_count") toInt
            )
        })

        resRdd.repartition(10).foreachPartition(upsertMongoBatch)


    }

    /**
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
     **/
    def upsertMongoBatch(crowd: Iterator[Crowd]): Unit = {


        val mongoClient = new MongoClient(mongoHost, mongoPort)
        val collectionRta = mongoClient.getDatabase(mongoDatabaseCrowd).getCollection(mongoCollectionRta)

        val upsertBatchSize = 5000
        var upsertIndexCount = 0
        val list = new util.ArrayList[WriteModel[Document]](upsertBatchSize)

        crowd.foreach(c => {
            upsertIndexCount += 1

            c match {
                case Crowd("imei_md5",imeiMd5,"alipay",imei,_,ocpxTag,ocpxTime,impCount,clkTime,clkCount) =>
                    val outDocument = new Document()
                    val setDocument = new Document()
                    val incDocument = new Document()

                    setDocument.append("imei_md5", imeiMd5)
                    if (imei != null && imei != "") setDocument.append("imei", imei)

                    if (clkTime != 0L) setDocument.append("alipay.clk_time", clkTime)
                    if (ocpxTime != 0L) setDocument.append("alipay.ocpx_time", ocpxTime)
                    if (ocpxTag != null && ocpxTag != "") setDocument.append("alipay.ocpx_tag", ocpxTag)

                    if (impCount != 0) incDocument.append("alipay.imp_count", impCount)
                    if (clkCount != 0) incDocument.append("alipay.clk_count", clkCount)

                    outDocument.append("$set", setDocument)
                    if (incDocument.size() != 0) {
                        outDocument.append("$inc", incDocument)
                    }

                    val updateBson = Filters.eq("imei_md5", imeiMd5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))

                    list.add(updateModel)

                case Crowd("imei_md5",imeiMd5,"dhh",imei,_,_,_,impCount,clkTime,clkCount) =>
                    val outDocument = new Document()
                    val setDocument = new Document()
                    val incDocument = new Document()

                    setDocument.append("imei_md5", imeiMd5)
                    if (imei != null && imei != "") setDocument.append("imei", imei)

                    if (clkTime != 0L) setDocument.append("dhh.clk_time", clkTime)
                    if (impCount != 0) incDocument.append("dhh.imp_count", impCount)
                    if (clkCount != 0) incDocument.append("dhh.clk_count", clkCount)
                    outDocument.append("$set", setDocument)
                    if (incDocument.size() != 0) {
                        outDocument.append("$inc", incDocument)
                    }

                    val updateBson = Filters.eq("imei_md5", imeiMd5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))

                    list.add(updateModel)
                case Crowd("idfa_md5",idfaMd5,"alipay",_,idfa,ocpxTag,ocpxTime,impCount,clkTime,clkCount) =>
                    val outDocument = new Document()
                    val setDocument = new Document()
                    val incDocument = new Document()

                    setDocument.append("idfa_md5", idfaMd5)
                    if(idfa != null && idfa != "") setDocument.append("idfa", idfa)

                    if (clkTime != 0L) setDocument.append("alipay.clk_time", clkTime)
                    if (ocpxTime != 0L) setDocument.append("alipay.ocpx_time", ocpxTime)
                    if (ocpxTag != null && ocpxTag != "") setDocument.append("alipay.ocpx_tag", ocpxTag)

                    if (impCount != 0) incDocument.append("alipay.imp_count", impCount)
                    if (clkCount != 0) incDocument.append("alipay.clk_count", clkCount)

                    //db.comment.update({_id:"2"},{$set:{likenum:NumberInt(889)}})
                    //db.comment.update({userid:"1003"},{$set:{nickname:"凯撒大帝"}},{multi:true})
                    outDocument.append("$set", setDocument)
                    if (incDocument.size() != 0) {
                        outDocument.append("$inc", incDocument)
                    }

                    val updateBson = Filters.eq("idfa_md5", idfaMd5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))

                    list.add(updateModel)
                case Crowd("idfa_md5",idfaMd5,"dhh",_,idfa,ocpxTag,ocpxTime,impCount,clkTime,clkCount) =>
                    val outDocument = new Document()
                    val setDocument = new Document()
                    val incDocument = new Document()

                    setDocument.append("idfa_md5", idfaMd5)

                    if(idfa != null && idfa != "") setDocument.append("idfa", idfa)
                    if (clkTime != 0L) setDocument.append("dhh.clk_time", clkTime)
                    if (impCount != 0) incDocument.append("dhh.imp_count", impCount)
                    if (clkCount != 0) incDocument.append("dhh.clk_count", clkCount)

                    outDocument.append("$set", setDocument)
                    if (incDocument.size() != 0) {
                        outDocument.append("$inc", incDocument)
                    }
                    val updateBson = Filters.eq("idfa_md5", idfaMd5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))
                    list.add(updateModel)
                case Crowd("oaid",oaid,"alipay",_,_,ocpxTag,ocpxTime,impCount,clkTime,clkCount) =>
                    val outDocument = new Document()
                    val setDocument = new Document()
                    val incDocument = new Document()

                    var oaidMd5:String = null
                    if(oaid.length == 32){ //md5
                        oaidMd5 = oaid
                        setDocument.append("oaid_md5",oaid)
                    }else{ //不是md5
                        oaidMd5 = getMd5(oaid)
                        setDocument.append("oaid", oaid)
                        setDocument.append("oaid_md5",oaidMd5)
                    }


                    if (clkTime != 0L) setDocument.append("alipay.clk_time", clkTime)
                    if (ocpxTime != 0L) setDocument.append("alipay.ocpx_time", ocpxTime)
                    if (ocpxTag != null && ocpxTag != "") setDocument.append("alipay.ocpx_tag", ocpxTag)

                    if (impCount != 0) incDocument.append("alipay.imp_count", impCount)
                    if (clkCount != 0) incDocument.append("alipay.clk_count", clkCount)

                    outDocument.append("$set", setDocument)
                    if (incDocument.size() != 0) {
                        outDocument.append("$inc", incDocument)
                    }

                    val updateBson = Filters.eq("oaid_md5", oaidMd5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))

                    list.add(updateModel)
                case Crowd("oaid",oaid,"dhh",_,_,_,_,impCount,clkTime,clkCount) =>
                    val outDocument = new Document()
                    val setDocument = new Document()
                    val incDocument = new Document()

                    var oaidMd5:String = null
                    if(oaid.length == 32){ //md5
                        oaidMd5 = oaid
                        setDocument.append("oaid_md5",oaid)
                    }else{ //不是md5
                        oaidMd5 = getMd5(oaid)
                        setDocument.append("oaid", oaid)
                        setDocument.append("oaid_md5",oaidMd5)
                    }


                    if (clkTime != 0L) setDocument.append("dhh.clk_time", clkTime)
                    if (impCount != 0) incDocument.append("dhh.imp_count", impCount)
                    if (clkCount != 0) incDocument.append("dhh.clk_count", clkCount)

                    outDocument.append("$set", setDocument)
                    if (incDocument.size() != 0) {
                        outDocument.append("$inc", incDocument)
                    }

                    val updateBson = Filters.eq("oaid_md5", oaidMd5)
                    val updateModel = new UpdateOneModel[Document](updateBson, outDocument, new UpdateOptions().upsert(true))

                    list.add(updateModel)
                case _ =>
            }



            if (upsertIndexCount % upsertBatchSize == 0) {

                collectionRta.bulkWrite(list)
                list.clear()
            }


        })// foreach 的括号

        if(list.size()!=0){
            collectionRta.bulkWrite(list)
            list.clear()
        }

        mongoClient.close()
    }


    def getMd5(content:String):String = {
        val md5 = MessageDigest.getInstance("MD5")
        val encoded = md5.digest((content).getBytes)
        encoded.map("%02x".format(_)).mkString
    }


    def insertClickhouse(dataframe:DataFrame,db:String,target_table:String)={
        val columns = dataframe.columns
        val coluName: String = columns.mkString(",")
        val nums = columns.map(_ => "?").mkString(",")

        val sql = s"insert into ${db}.${target_table} (${coluName}) values (${nums})"
           println(sql)

        val schema = dataframe.schema
        val fields = schema.fields

        dataframe.rdd.foreachPartition { iter =>
            var conn: ClickHouseConnection = null
            var pstmt: PreparedStatement = null

            try {
                Class.forName(ClickHouse.driver)
                val source = new ClickHouseDataSource(s"jdbc:clickhouse://${ClickHouse.host}:${ClickHouse.port}")
                conn = source.getConnection(ClickHouse.user, ClickHouse.password)
                pstmt = conn.prepareStatement(sql)


                var count = 0
                iter.foreach { row =>
                    fields.foreach { field =>
                        val index = schema.fieldIndex(field.name)
                        var value = row.get(index)
                        if (null == value) value = defaultNullValue(field.dataType)
                        pstmt.setObject(index + 1, value)
                    }

                    pstmt.addBatch()
                    count += 1
                    if (count > 1000){
                        pstmt.executeBatch()
                        count = 0
                    }

                }

                pstmt.executeBatch()

            } catch {
                case e => e.printStackTrace()
            }

           if(null != pstmt) pstmt.close()
           if (null != conn) conn.close()

        }

        dataframe.show(1)

    }


    def defaultNullValue(dataType:DataType):Any={
        dataType match {
            case DecimalType() => 0D
            case FloatType => 0F
            case DoubleType => 0.0
            case IntegerType => 0
            case LongType => 0L
            case StringType => null
            case BooleanType => false
            case _ => null
        }
    }


}
