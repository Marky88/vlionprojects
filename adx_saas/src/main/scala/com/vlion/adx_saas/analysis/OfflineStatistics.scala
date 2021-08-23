package com.vlion.adx_saas.analysis

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.vlion.adx_saas.jdbc.MySQL
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @description:
 * @author: malichun
 * @time: 2021/7/8/0008 17:48
 *
 */
object OfflineStatistics {

    val mysqlStateTableName = "stat"

    /**
     * 更新myql 的pkg
     *
     * @param spark
     * @param etlDate
     * @param etlHour
     */
    def updateMysqlPkg(implicit spark: SparkSession, etlDate: String, etlHour: String): Unit = {
        spark.sql(
            s"""
               |select distinct pkg_name from ods.adx_saas_media_req  where pkg_name is not null and pkg_name!=''
               |and etl_date = '${etlDate}' and etl_hour = '$etlHour'
               |""".stripMargin)
            .rdd
            .repartition(2)
            .map(r => r.getAs[String]("pkg_name"))
            .filter(_.length < 500) // 长度超长了
            .foreachPartition(iter => {
                Class.forName(MySQL.driver)
                val conn = DriverManager.getConnection(MySQL.url, MySQL.user, MySQL.password)
                val pstmt = conn.prepareStatement("insert into pkg (name) values(?) ON DUPLICATE key update name=values(name)")

                (pstmt /: iter) ((pstmt, line) => {
                    pstmt.setString(1, line)
                    pstmt.execute()
                    pstmt
                })

                pstmt.close()
                conn.close()
            })
    }

    def hourSummary(implicit spark: SparkSession, etlDate: String, etlHour: String): Unit = {

        //        val sdfDay = new SimpleDateFormat("yyyy-MM-dd")
        //        val dayTimestamp = sdfDay.parse(s"$etlDate").getTime / 1000 -57600 // 转换成utc时间的天

        // 当前小时的时间戳
        //        val dayHourTimestamp = new SimpleDateFormat("yyyy-MM-dd HH").parse(s"$etlDate $etlHour").getTime / 1000
        //
        //        val sdfDay = new SimpleDateFormat("yyyy-MM-dd")
        //        sdfDay.setTimeZone(TimeZone.getTimeZone("UTC")); // 使用UTC的天,小时的时间戳按照当前的就行了,反正是时间戳
        //        val dayTimestamp = sdfDay.parse(sdfDay.format(new Date(dayHourTimestamp * 1000))).getTime/1000
        import org.apache.spark.sql.functions.lit
        val resDF = (spark table "media_req" union (spark.table("uniontable")).where(
            """target_id is not null and
              |target_id != '' and
              |time is not null and
              |dsp_id is not null and
              |dsp_id != '' and
              |pkg_id is not null and media_id is not null""".stripMargin)
            )
            .withColumn("update_time", lit(s"${etlDate} ${etlHour}:00:00"))
            .withColumn("del", lit("no")).repartition(500)
                .createOrReplaceTempView("resDF")

        val resDF2 = spark.sql( // 有正则,比较麻烦,使用raw插值
            raw"""
               |select
               |    time,
               |    hour,
               |    time_format,
               |    dsp_id,
               |    target_id,
               |    media_id,
               |    posid_id,
               |    pkg_id,
               |    country_id,
               |    platform_id,
               |    style_id,
               |    mlevel_id,
               |    del,
               |    update_time,
               |    max(ssp_req) as ssp_req,
               |    max(dsp_req) as dsp_req,
               |    max(dsp_fill_req) as dsp_fill_req,
               |    max(dsp_win) as dsp_win,
               |    max(ssp_win) as ssp_win,
               |    max(dsp_floor) as dsp_floor,
               |    max(ssp_floor) as ssp_floor,
               |    max(dsp_win_price) as dsp_win_price,
               |    max(imp) as imp,
               |    max(clk) as clk,
               |    max(revenue) as revenue,
               |    max(cost) as cost,
               |    max(dsp_req_timeout) as dsp_req_timeout,
               |    max(dsp_req_parse_error) as dsp_req_parse_error,
               |    max(dsp_req_invalid_ad) as dsp_req_invalid_ad,
               |    max(dsp_req_no_bid) as dsp_req_no_bid
               |from
               |    resDF
               |    where posid_id  rlike '^[\\d-\\.]+$$'
               |    and media_id rlike '^[\\d-\\.]+$$'
               |    and dsp_id rlike '^[\\d-\\.]+$$'
               |    and pkg_id rlike '^[\\d-\\.]+$$'
               |    and country_id rlike '^[\\d-\\.]+$$'
               |    and platform_id rlike '^[\\d-\\.]+$$'
               |    and style_id rlike '^[\\d-\\.]+$$'
               |    and mlevel_id rlike '^[\\d-\\.]+$$'
               |group by
               |    time,hour,time_format,dsp_id,target_id,media_id,posid_id,pkg_id,country_id,platform_id,style_id,mlevel_id,del,update_time
               |""".stripMargin)
//            .coalesce(5)
            .persist(StorageLevel.MEMORY_AND_DISK)


        //        val prop=new java.util.Properties()
        //        prop.put("driver",MySQL.driver)
        //        prop.put("user",MySQL.user)
        //        prop.put("password",MySQL.password)
        //        resDF
        //            .coalesce(5)
        //            .write
        //            .jdbc(MySQL.url,mysqlStateTableName,prop)

//        insertUpdateMysql(spark, resDF, 12, mysqlStateTableName)

        // 导入到impala
        resDF2.coalesce(1).createOrReplaceTempView("resDF2")

        // 导入到impala/hive表中
        spark.sql(
            s"""
               |insert overwrite table dm.adx_saas_stat
               |partition(etl_date='$etlDate',etl_hour='$etlHour')
               |select
               |time,
               |hour,
               |time_format,
               |dsp_id,
               |target_id,
               |media_id,
               |posid_id,
               |pkg_id,
               |country_id,
               |platform_id,
               |style_id,
               |mlevel_id,
               |ssp_req,
               |dsp_req,
               |dsp_fill_req,
               |dsp_win,
               |ssp_win,
               |dsp_floor,
               |ssp_floor,
               |dsp_win_price,
               |imp,
               |clk,
               |revenue,
               |cost,
               |dsp_req_timeout,
               |dsp_req_parse_error,
               |dsp_req_invalid_ad,
               |dsp_req_no_bid,
               |del
               |from resDF2
               |""".stripMargin)


        try{
            // 导入到mysql
            resDF2
                .repartition(10)
                .write.mode("append")
                .format("jdbc")
                .option("url", MySQL.url)
                .option("driver", MySQL.driver)
                .option("user", MySQL.user)
                .option("password", MySQL.password)
                .option("dbtable", mysqlStateTableName)
                .save()
        }catch {
            case e:Exception => e.printStackTrace()
        }


    }


    private def insertUpdateMysql(spark: SparkSession, df: DataFrame, keyNum: Int, targetTable: String) = {
        val allColsSeq = df.schema.map(sf => sf.name).toList
        val broadcast = spark.sparkContext.broadcast(allColsSeq)

        df.rdd.coalesce(4).foreachPartition(iter => {
            val list = iter.toList
            //            println("list.size: "+list.size)

            val allColsSeq = broadcast.value
            val allCols = allColsSeq.mkString(",")
            val updateCols = allColsSeq.slice(keyNum, allColsSeq.size).map(col => s"${col}=values(${col})").mkString(",")

            Class.forName(MySQL.driver)
            val conn = DriverManager.getConnection(MySQL.url, MySQL.user, MySQL.password)

            val `num?` = allColsSeq.indices.map(_ => "?").mkString(",")

            val pstmt = conn.prepareStatement(s"insert into $targetTable (${allCols}) values(${`num?`}) ON DUPLICATE key update ${updateCols}")
            //            println(s"insert into $targetTable (${allCols}) values(${`num?`}) ON DUPLICATE key update ${updateCols}")
            val colTuple = allColsSeq.zipWithIndex.map(t => (t._1, t._2 + 1))
            //            println("colTuple: "+colTuple)

            list map (row => {
                //                println("=" * 10)
                //                println(row)
                //                println("=" * 10)

                colTuple foreach { case (colName, pstmtIndex) =>
                    // println(pstmtIndex,row.getAs[String](colName))
                    // 插入每条数据
                    pstmt.setObject(pstmtIndex, row.getAs[String](colName))
                }
                pstmt.execute()
            })
            pstmt.close()
            conn.close()
        })
    }


    def hourSummaryTest(spark: SparkSession, dayTimestamp: String, dayHourTimestamp: String, etlDate: String,etlHour:String): Unit = {
        val resDF = spark.sql(
            s"""
               |(
               |select
               |    ${dayTimestamp} as time,
               |    $dayHourTimestamp as hour,
               |    '$etlDate' as time_format, -- 选取的是北京的日期
               |    if(dsp_id is null or dsp_id='','-1',dsp_id) as dsp_id,
               |    if(media_id    is null or media_id     ='','-1',media_id     ) as  media_id   ,
               |    if(posid_id    is null or posid_id     ='','-1',posid_id     ) as  posid_id   ,
               |    if(pkg_id      is null or pkg_id       ='','-1',pkg_id       ) as  pkg_id     ,
               |    if(country_id  is null or country_id   ='','-1',country_id   ) as  country_id ,
               |    if(platform_id is null or platform_id  ='','-1',platform_id  ) as  platform_id,
               |    if(style_id    is null or style_id     ='','-1',style_id     ) as  style_id   ,
               |    if(mlevel_id   is null or mlevel_id    ='','-1',mlevel_id    ) as  mlevel_id,
               |    null as ssp_req,
               |    sum(dsp_req) as dsp_req,
               |    sum(dsp_fill_req) as dsp_fill_req,
               |    sum(dsp_win) as dsp_win,
               |    sum(ssp_win) as ssp_win,
               |    sum(sum_dsp_floor_price) as dsp_floor,
               |    null as ssp_floor,
               |    sum(dsp_win_price) as dsp_win_price,
               |    sum(imp) as imp,
               |    sum(clk) as clk,
               |    sum(revenue) as revenue,
               |    sum(cost) as cost,
               |    sum(dsp_req_timeout) as dsp_req_timeout,
               |    sum(dsp_req_parse_error) as dsp_req_parse_error,
               |    sum(dsp_req_invalid_ad) as dsp_req_invalid_ad,
               |    sum(dsp_req_no_bid) as dsp_req_no_bid,
               |    'no' as del
               |    from
               |    (
               |        select
               |            t1.request_id,
               |            t2.dsp_id, -- 切换回来
               |            t1.media_id,
               |            t1.posid_id,
               |            t1.pkg_id,
               |            t1.country_id,
               |            t1.platform_id,
               |            t1.style_id,
               |            t1.mlevel_id,
               |            null as ssp_req, -- ssp_req,有问题..,-1的数据
               |            t1.dsp_req,
               |            t1.dsp_fill_req,
               |            t1.dsp_win,
               |            t1.ssp_win,
               |            t1.sum_dsp_floor_price,
               |            null as ssp_floor,   -- -1的数据
               |            t1.dsp_win_price,
               |            t1.imp,
               |            t1.clk,
               |            t1.revenue,
               |            t1.cost,
               |            t1.dsp_req_timeout,
               |            t1.dsp_req_parse_error,
               |            t1.dsp_req_invalid_ad,
               |            t1.dsp_req_no_bid
               |        from
               |        union_table t1
               |        left join
               |        target t2
               |        on t1.dsp_id = t2.id
               |    ) t
               |group by
               |    if(dsp_id is null or dsp_id='','-1',dsp_id) ,
               |    if(media_id    is null or media_id     ='','-1',media_id     )   ,
               |    if(posid_id    is null or posid_id     ='','-1',posid_id     )   ,
               |    if(pkg_id      is null or pkg_id       ='','-1',pkg_id       )    ,
               |    if(country_id  is null or country_id   ='','-1',country_id   ) ,
               |    if(platform_id is null or platform_id  ='','-1',platform_id  ) ,
               |    if(style_id    is null or style_id     ='','-1',style_id     )    ,
               |    if(mlevel_id   is null or mlevel_id    ='','-1',mlevel_id    )
               |)
               |union all
               |(
               |    select
               |    ${dayTimestamp} as time,
               |    $dayHourTimestamp as hour,
               |    '$etlDate' as time_format, -- 选取的是北京的日期
               |    -2 as dsp_id,
               |    if(media_id    is null or media_id     ='','-1',media_id     ) as  media_id   ,
               |    if(posid_id    is null or posid_id     ='','-1',posid_id     ) as  posid_id   ,
               |    if(pkg_id      is null or pkg_id       ='','-1',pkg_id       ) as  pkg_id     ,
               |    if(country_id  is null or country_id   ='','-1',country_id   ) as  country_id ,
               |    if(platform_id is null or platform_id  ='','-1',platform_id  ) as  platform_id,
               |    if(style_id    is null or style_id     ='','-1',style_id     ) as  style_id   ,
               |    if(mlevel_id   is null or mlevel_id    ='','-1',mlevel_id    ) as  mlevel_id,
               |        sum(ssp_req) as ssp_req,
               |        null as dsp_req,
               |        null dsp_fill_req,
               |        null dsp_win,
               |        null ssp_win,
               |        null as dsp_floor,
               |        sum(sum_ssp_floor_price) as ssp_floor,
               |        null as dsp_win_price,
               |        null as imp,
               |        null as clk,
               |        null as revenue,
               |        null as cost,
               |        null as dsp_req_timeout,
               |        null dsp_req_parse_error ,
               |        null dsp_req_invalid_ad,
               |        null dsp_req_no_bid,
               |        'no' as del
               |    from
               |    meidiaReq
               |    group by
               |    if(media_id    is null or media_id     ='','-1',media_id     )   ,
               |    if(posid_id    is null or posid_id     ='','-1',posid_id     )   ,
               |    if(pkg_id      is null or pkg_id       ='','-1',pkg_id       )    ,
               |    if(country_id  is null or country_id   ='','-1',country_id   ) ,
               |    if(platform_id is null or platform_id  ='','-1',platform_id  ) ,
               |    if(style_id    is null or style_id     ='','-1',style_id     )    ,
               |    if(mlevel_id   is null or mlevel_id    ='','-1',mlevel_id    )
               |)
               |""".stripMargin)
        resDF.persist(StorageLevel.MEMORY_AND_DISK)

        resDF.show(false)
        resDF
            .coalesce(5)
            .write.mode("append")
            .format("jdbc")
            .option("url", MySQL.url)
            .option("driver", MySQL.driver)
            .option("user", MySQL.user)
            .option("password", MySQL.password)
            .option("dbtable", mysqlStateTableName)
            .save()

    }
}
