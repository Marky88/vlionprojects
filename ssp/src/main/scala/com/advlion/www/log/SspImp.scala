package com.advlion.www.log

/**
 *
 * @param time 时间戳
 * @param dspId dspId
 * @param appId 媒体id
 * @param tagId 广告位id
 * @param ip ip
 * @param iMei iMei
 * @param idFa idFa
 */
//case class SspImp(time:Int,dspId:Int,appId:Int,tagId:Int,ip:String,iMei:String,idFa:String)
//case class SspImp(time:String,dspId:String,appId:String,tagId:String)
//case class SspImp(time:Int,dspId:Int,appId:Int,tagId:Int)
case class SspImp(time:String,dspId:String,appId:String,tagId:String,ip:String,iMei:String,idFa:String)

