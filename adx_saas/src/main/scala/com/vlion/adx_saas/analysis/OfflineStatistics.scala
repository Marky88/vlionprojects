package com.vlion.adx_saas.analysis

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}

import com.vlion.adx_saas.jdbc.MySQL
import org.apache.spark.sql.SparkSession

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
               |select distinct pkg_name from ods.adx_saas_media_req where pkg_name is not null and pkg_name!=''
               |""".stripMargin)
            .rdd
            .repartition(2)
            .map(r => r.getAs[String]("pkg_name"))
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
        val dayHourTimestamp = new SimpleDateFormat("yyyy-MM-dd HH").parse(s"$etlDate $etlHour").getTime / 1000

        val sdfDay = new SimpleDateFormat("yyyy-MM-dd")
        sdfDay.setTimeZone(TimeZone.getTimeZone("UTC")); // 使用UTC的天,小时的时间戳按照当前的就行了,反正是时间戳
        val dayTimestamp = sdfDay.parse(sdfDay.format(new Date(dayHourTimestamp * 1000))).getTime/1000

        val resDF = spark.sql(
            s"""
               | -- 补充-1
               |select
               |    time,
               |    hour,
               |    time_format,
               |    if(dsp_id is null or dsp_id='','-1',dsp_id) as dsp_id,
               |    if(media_id    is null or media_id     ='','-1',media_id     ) as  media_id   ,
               |    if(posid_id    is null or posid_id     ='','-1',posid_id     ) as  posid_id   ,
               |    if(pkg_id      is null or pkg_id       ='','-1',pkg_id       ) as  pkg_id     ,
               |    if(country_id  is null or country_id   ='','-1',country_id   ) as  country_id ,
               |    if(platform_id is null or platform_id  ='','-1',platform_id  ) as  platform_id,
               |    if(style_id    is null or style_id     ='','-1',style_id     ) as  style_id   ,
               |    if(mlevel_id   is null or mlevel_id    ='','-1',mlevel_id    ) as  mlevel_id,
               |    dsp_req,
               |    dsp_fill_req,
               |    dsp_win,
               |    ssp_win,
               |    dsp_floor,
               |    ssp_floor,
               |    dsp_qps,
               |    dsp_win_price,
               |    imp,
               |    clk,
               |    revenue,
               |    cost,
               |    dsp_req_timeout,
               |    dsp_req_parse_error,
               |    dsp_req_invalid_ad,
               |    dsp_req_no_bid,
               |    del
               |from
               |(
               |select
               |    ${dayTimestamp} as time,
               |    $dayHourTimestamp as hour,
               |    '$etlDate' as time_format, -- 选取的是北京的日期
               |    dsp_id,
               |    media_id,
               |    posid_id,
               |    pkg_id,
               |    country_id,
               |    platform_id,
               |    style_id,
               |    mlevel_id,
               |    sum(dsp_req) as dsp_req,
               |    sum(dsp_fill_req) as dsp_fill_req,
               |    count(dsp_win) as dsp_win,
               |    count(ssp_win) as ssp_win,
               |    sum(sum_dsp_floor_price) / sum(count_dsp_floor_price) as dsp_floor,
               |    avg(sum_ssp_floor_price) as ssp_floor,
               |    sum(dsp_req) / 3600 as dsp_qps,
               |    avg(revenue) as dsp_win_price,
               |    count(imp) as imp,
               |    count(clk) as clk,
               |    sum(revenue) as revenue,
               |    sum(cost) as cost,
               |    sum(dsp_req_timeout) as dsp_req_timeout,
               |    sum(dsp_req_parse_error) as dsp_req_parse_error,
               |    sum(dsp_req_invalid_ad) as dsp_req_invalid_ad,
               |    sum(dsp_req_no_bid) as dsp_req_no_bid,
               |    'no' as del
               |from
               |    union_table
               |group by
               |    dsp_id,
               |    media_id,
               |    posid_id,
               |    pkg_id,
               |    country_id,
               |    platform_id,
               |    style_id,
               |    mlevel_id
               |) t
               |""".stripMargin)

        resDF.show(false)
        resDF
            .coalesce(5)
            .write.mode("append")
            .format("jdbc")
            .option("url",MySQL.url)
            .option("driver",MySQL.driver)
            .option("user",MySQL.user)
            .option("password",MySQL.password)
            .option("dbtable",mysqlStateTableName)
            .save()

    }

}
