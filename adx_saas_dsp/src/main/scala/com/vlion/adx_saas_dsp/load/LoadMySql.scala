package com.vlion.adx_saas_dsp.load

import com.vlion.adx_saas_dsp.jdbc.MySQL
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/7/7/0007 17:24
 *
 */
object LoadMySql {
    val dspCampaignFakeTable = "dsp_campaign_fake"

    def readMysql(spark: SparkSession) = {
        val url = MySQL.url
        val user = MySQL.user
        val password = MySQL.password


        spark.read.format("jdbc")
            .option("url", url)
            .option("user", user)
            .option("password", password)
            .option("dbtable",dspCampaignFakeTable)
            .load()
            .cache()
            .createOrReplaceTempView("dsp_campaign_fake")
    }


}
