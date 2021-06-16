package com.vlion.sparkstreaming.projectlearn.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @author malichun
 * @date 2020/7/23 002315:25
 */
object PropertiesUtil {
    def load(propertiesName:String):Properties= {
        val prop = new Properties()
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),"UTF-8"))
        prop
    }
}
