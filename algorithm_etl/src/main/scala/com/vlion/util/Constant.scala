package com.vlion.util

import java.util.Properties

/**
 * @description:
 * @author: malichun
 * @time: 2021/8/16/0016 19:45
 *
 */
object Constant {
    val prop = new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("config.properties"))
    val appIds = prop.getProperty("app_ids").split(",").map(_ trim)

}
