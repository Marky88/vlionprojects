package com.vlion.adx_saas_dsp.jdbc

import java.util.Properties

/**
 * @description:
 * @author:
 * @time: 2021/11/9/0009 12:06
 *
 */
object ClickHouse {
     val props: Properties = new Properties()
     props.load(this.getClass.getClassLoader.getResourceAsStream("clickhouse.properties"))

     val driver: String = props.getProperty("driver")
     val host: String = props.getProperty("host")
     val port: String = props.getProperty("port")
     val database: String = props.getProperty("database")
     val user: String = props.getProperty("user")
     val password: String = props.getProperty("password")
     val table: String = props.getProperty("table")


/*

    def main(args: Array[String]): Unit = {
        println(ClickHouse.host)
        println(ClickHouse.database)
        println(ClickHouse.table)
    }
*/


}
