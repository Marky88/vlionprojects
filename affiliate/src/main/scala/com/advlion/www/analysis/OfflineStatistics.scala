package com.advlion.www.analysis

import com.advlion.www.jdbc.MySQL
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2020/11/5/0005 12:56
 *
 */
object OfflineStatistics {
    val url: String = MySQL.url
    val user: String  = MySQL.user
    val password: String  = MySQL.password
    val driver: String  = MySQL.driver
    val resultTable = "stat_tmp"

    def summary(spark:SparkSession,etlDate:String, etlHour:String): Unit = {
        val resultDF = spark.sql(
            s"""
               |select
               |    '$etlDate' as time,
               |	media_id as channel_id,
               |	offer_id as adv_id,
               |	source_id as aduser_id,
               |	sub_channel as subchannel,
               |	'n' as is_upload,			     --is_upload
               |	sum(click_count) as click_count,
               |	sum(activate_count) as activate_count,
               |	sum(shave_activates) as shave_activates,
               |	'40' as county_id     --香港日志标记id
               |from
               |unionDF
               |group by
               |    media_id,offer_id,source_id,sub_channel
               |""".stripMargin)

        resultDF.show()

        resultDF.write.mode("overwrite")
            .format("jdbc")
            .option("url",url)
            .option("driver",driver)
            .option("user",user)
            .option("password",password)
            .option("dbtable",resultTable)
            .save()

    }
}
