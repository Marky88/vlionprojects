package com.advlion.www.util;

import java.io.InputStreamReader
import java.util.Properties;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/29/0029 13:29
 */
object PropertiesUtil {
    def load(propertiesName:String):Properties= {
        val prop = new Properties()
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),"UTF-8"))
        prop
    }
}
