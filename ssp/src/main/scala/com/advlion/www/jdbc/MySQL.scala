package com.advlion.www.jdbc

import java.util.Properties

/**
  * Created by Admin on 2020/4/16.
  */
object MySQL {
  val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("mysql.properties"))
  val url: String = prop.getProperty("url")
  val user: String = prop.getProperty("user")
  val password: String = prop.getProperty("password")
  val driver: String = prop.getProperty("driver")

  def main(args: Array[String]): Unit = {
    println(MySQL.url)
  }
}
