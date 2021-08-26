package com.advlion.www.realtime.model

/**
 * 服务器地址：47.88.85.171
 *
 * 日志路径：/data/programs/api.alloffb.com/logs/
 *
 * 日志文件名格式：access_{YYYY-MM-DD}.log，日志保留5天
 */
case class Alloffb_log(date:String,
                       hour:String,
                       ip:String,
                       url:URL_param,
                       respCode:String,
                       processTime:String,
                       respStrLength:String,
                       userAgent:String
                      ) extends Serializable

case class URL_param(event:String,ad:String,ver:String,uid:String,os:String,msg:String) extends Serializable