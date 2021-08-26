package com.advlion.www.realtime.parse

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.advlion.www.realtime.model.{Alloffb_log, URL_param}

import scala.collection.mutable


/**
 * 日志格式
 * 1:"120.36.224.120"
 * 2:-
 * 3:[22/Jul/2020:00:08:30 -0700]"PUT /api/v1/adsync?ad=217726882693530_217727316026820&appkey=6352e2c40c14639e&event=0&msg=&os=android&ts=1595401705&uid=fadeb78d-ca92-4e1d-bde8-2aa20e45df6b&ver=5.9.0&sign=64c9ef0e09fa351f4ea63833030d91b6 HTTP/1.1"
 * 4:200
 * 5:0.000
 * 6:32
 * 7:"-"
 * 8:"Dalvik/2.1.0 (Linux; U; Android 10; Redmi Note 8 Pro MIUI/V11.0.3.0.QGGCNXM)"
 * 9:"-"
 *
 * case class Alloffb_log(
 * date:String,
 * hour:String,
 * ip:String,
 * url:URL_param,
 * respCode:String,
 * processTime:String,
 * respStrLength:Int,
 * userAgent:String
 * )
 *
 * case class URL_param(event:String,ad:String,ver:String,uid:String,os:String,msg:String)
 */
object ParseAlloffbLog extends Serializable {

    def parse(str:String):Alloffb_log={
        if(str == null){
            return null
        }
        val arr = str.split("\\t",-1)
        if(arr.length != 9){
            return null
        }

        val ip=arr(0)
        val urlStr= arr(2)
        val respCode=arr(3)
        val processTime =arr(4)
        val respStrLength=arr(5)
        val userAgent=arr(7)
        try {
            val tuple = parseURL(str)
            Alloffb_log(tuple._1,tuple._2,ip,URL_param(tuple._3,tuple._4,tuple._5,tuple._6,tuple._7,tuple._8),respCode,processTime,respStrLength,userAgent)
        } catch{
            case e:Exception =>  null
        }


    }

    //[22/Jul/2020:00:08:30 -0700]"PUT /api/v1/adsync?ad=217726882693530_217727316026820&appkey=6352e2c40c14639e&event=0&msg=&os=android&ts=1595401705&uid=fadeb78d-ca92-4e1d-bde8-2aa20e45df6b&ver=5.9.0&sign=64c9ef0e09fa351f4ea63833030d91b6 HTTP/1.1"
     def parseURL(urlStr:String)={
         val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
         val daySdf=new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH)
         val hourSdf= new SimpleDateFormat("HH", Locale.ENGLISH)


         //  22/Jul/2020:00:08:30 -0700
        //  27/Jul/2020:01:29:38
        val timeStr = urlStr.substring(urlStr.indexOf('[')+1, urlStr.indexOf(']')-6)
//        println("time:Str:"+timeStr)
        val date = sdf.parse(timeStr)
        val day=daySdf.format(date)
        val hour=hourSdf.format(date)

        val params = urlStr.substring(urlStr.indexOf('?') + 1, urlStr.lastIndexOf(' '))
        val strings = params.split("&",-1)
        val map = strings.foldLeft(mutable.HashMap[String, String]())((map, ele) => {
            val kv = ele.split("=", -1)
            if(kv.length==2){
                map.put(kv(0), kv(1))
            }

            map
        })
        (day,hour,map.getOrElse("event",null),map.getOrElse("ad",null),map.getOrElse("ver",null),map.getOrElse("uid",null),map.getOrElse("os",null),map.getOrElse("msg",null))
    }

    def main(args: Array[String]): Unit = {

//        println(sdf.parse("27/Jul/2020:01:42:13"))
        val arr=Array(
            "\"103.242.23.182\"\t-\t[27/Jul/2020:01:34:19 -0700]\"PUT /api/v1/adsync?ad=37a1a4bc-9ebf-42ba-9008-12e4f6cb7ed3&appkey=6352e2c40c14639e&event=3&msg=&os=android&ts=1595838857&uid=4b628c61-7069-4391-8434-6dd0fc1bf8e2&ver=5.9.0&sign=757d24ebab4bd9300ff90db120d136ea HTTP/1.1\"\t200\t0.000\t32\t\"-\"\t\"Dalvik/2.1.0 (Linux; U; Android 7.1; Q3 Build/LMY47I)\"\t\"-\""
            ,"\"103.242.23.182\"\t-\t[27/Jul/2020:01:34:17 -0700]\"PUT /api/v1/adsync?ad=217726882693530_217727316026820&appkey=6352e2c40c14639e&event=1&msg=&os=android&ts=1595838855&uid=4b628c61-7069-4391-8434-6dd0fc1bf8e2&ver=5.9.0&sign=8bfe918612ad353971834a36e05145b8 HTTP/1.1\"\t200\t0.000\t32\t\"-\"\t\"Dalvik/2.1.0 (Linux; U; Android 7.1; Q3 Build/LMY47I)\"\t\"-\""
            ,"\"223.204.221.160\"\t-\t[27/Jul/2020:01:34:22 -0700]\"PUT /api/v1/adsync?ad=217726882693530_217727316026820&appkey=6352e2c40c14639e&event=0&msg=&os=android&ts=1595838860&uid=f7dcc3f0-1545-413d-a23d-cc4407577433&ver=5.9.0&sign=3023d57d2b36e73edbd852ad7811899d HTTP/1.1\"\t200\t0.000\t32\t\"-\"\t\"Dalvik/2.1.0 (Linux; U; Android 7.1; U2 Build/LMY47I)\"\t\"-\""

        )
        arr.foreach( s => {
            val alloffb_log = parse(s)
            println(alloffb_log)

        })
    }
}
