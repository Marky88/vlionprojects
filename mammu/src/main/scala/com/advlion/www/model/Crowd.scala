package com.advlion.www.model

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/30/0030 10:32
 *
 */
case class Crowd(
                    deviceType:String,  // imei_md5,idfa_md5,oaid
                    deviceValue:String,
                    modelType: String,
                    imei: String,
                    idfa: String,
                    ocpxTag: String,
                    ocpxTime: Long,
                    impCount: Int,
                    clkTime: Long,
                    clkCount: Int
                )
