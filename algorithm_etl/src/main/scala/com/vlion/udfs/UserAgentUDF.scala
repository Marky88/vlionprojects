package com.vlion.udfs

import nl.bitwalker.useragentutils.UserAgent

/**
 * @description:
 * @author: malichun
 * @time: 2021/8/24/0024 11:13
 *       转换uerAgent
 *
 */
object UserAgentUDF {
    def convertUa(uaStr:String): String ={
        if(uaStr == null || uaStr.trim == ""){
            return ""
        }
        val userAgent = UserAgent.parseUserAgentString(uaStr)
        var name = userAgent.getBrowser.getGroup.getName
        if(name contains(" ")){
            name = name.split(" ")(0)
        }

//        println("version:"+userAgent.getBrowserVersion)
        val version = userAgent.getBrowserVersion match {
            case x if x != null => userAgent.getBrowserVersion.getMajorVersion
            case _ => ""
        }

        name + version
    }

    def main(args: Array[String]): Unit = {
        println(convertUa("Mozilla/5.0 (iPhone; CPU iPhone OS 13_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"))

    }

}
