package com.vlion.adx_saas.load

import com.vlion.adx_saas.jdbc.MySQL
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/7/7/0007 17:24
 *
 */
object LoadMySql {

    def readMysql(implicit spark: SparkSession) = {
         val url = MySQL.url
         val user = MySQL.user
        val password = MySQL.password

        def genreate2(tableName:String) ={
            generateMysqlTableView(tableName)(spark,url,user,password) // 生成id -> dsp_id
        }

        genreate2("target") // 生成id -> dsp_id
        genreate2("country")
        genreate2("platform")
        genreate2("style")
        genreate2("media")
        genreate2("pkg")
    }

    private def generateMysqlTableView(mysqlTableName: String)(implicit spark:SparkSession,url: String, user: String, password: String) = {
        spark.read.format("jdbc")
            .option("url", url)
            .option("user", user)
            .option("password", password)
            .option("dbtable", mysqlTableName)
            .load()
            .cache()
            .createOrReplaceTempView(mysqlTableName)
    }


}
