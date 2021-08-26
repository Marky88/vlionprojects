package com.advlion.www.parse

import scala.collection.mutable


object SAAccessLogParse {
    /**
     * 解析
     * GET /api/v1/sa?act=call&domain=aisungames.com&chid=005&template_id=template8&page=detail.html&ad_flag=detail2&_id=636db04dc4a92f52eed6c3a8b95eeebc HTTP/1.1
     * @param requestStr
     * @return
     */
    def parseLog(requestStr:String):Map[String,String] ={


        val str = requestStr.substring(requestStr.indexOf('?')+1, requestStr.lastIndexOf(' '))
        if(str==null || str.equals("")){
            null
        }else{
            val map:mutable.Map[String,String]= new mutable.HashMap[String,String]()
            val arr = str.split("&")
            arr.foreach(s =>{
                val arr2 = s.split("=")
                if(arr2.length==2){
                    val key=arr2(0)
                    val value=arr2(1)
                    map(key)=value
                }
            })
            map.toMap
        }

    }

    def main(args: Array[String]): Unit = {
        val aa = parseLog("GET /api/v1/sa?act=call&domain=aisungames.com&chid=005&template_id=template8&page=detail.html&ad_flag=detail2&_id=636db04dc4a92f52eed6c3a8b95eeebc HTTP/1.1")
        aa.foreach(println)
    }

}
