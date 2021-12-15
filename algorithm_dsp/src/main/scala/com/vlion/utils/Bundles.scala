package com.vlion.utils

import java.util.Properties

/**
 * @description:
 * @author:
 * @time: 2021/12/9/0009 14:06
 *
 */
object Bundles {
     val props: Properties = new Properties()
     props.load(this.getClass.getClassLoader.getResourceAsStream("config.properties"))
     val bundles: Array[String] = props.getProperty("bundles").split(",").map(_.trim)

}
