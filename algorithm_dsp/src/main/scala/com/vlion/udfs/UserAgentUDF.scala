package com.vlion.udfs

import java.net.URLDecoder

import nl.bitwalker.useragentutils.UserAgent

/**
 * @description:
 * @author:
 * @time: 2021/12/7/0007 15:31
 *
 */
object UserAgentUDF {

    def decodeURL(url:String):String={
        //日志URL编码,这边解码
      val  decodeStr = URLDecoder.decode(url,"UTF-8")
        decodeStr
    }

    def convertUa(uaStr:String):String = {
        if (uaStr == null || uaStr == ""){
            return ""
        }



        val agent = UserAgent.parseUserAgentString(uaStr)
        val name= agent.getBrowser().getGroup().getName()
        if (name.contains(" ")){
            val str = name.split(" ")(0)
        }


        val version = agent.getBrowserVersion() match {
            case x if x != null => agent.getBrowserVersion.getMajorVersion
            case _ => ""
        }

        name + version
    }


    def main(args: Array[String]): Unit = {
        print(convertUa("Mozilla/5.0 (iPhone; CPU iPhone OS 13_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"))

    }

}
