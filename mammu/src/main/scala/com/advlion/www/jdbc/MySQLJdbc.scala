package com.advlion.www.jdbc

import java.util.Properties

object MySQLJdbc {
  val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("mysql.properties"))
  val url: String = prop.getProperty("url")
  val user: String = prop.getProperty("user")
  val password: String = prop.getProperty("password")
  val driver: String = prop.getProperty("driver")
}
