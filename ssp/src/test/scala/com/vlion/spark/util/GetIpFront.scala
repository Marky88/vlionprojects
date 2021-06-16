package com.vlion.spark.util

/**
 * @description:
 * @author: malichun
 * @time: 2020/12/11/0011 17:58
 *
 */
object GetIpFront {

val getIPFront = (ip:String) => {
        if(ip ==null ){
            null
        }else{
            val arr=ip.split("\\.")
            if(arr.length != 4){
                null
            }else{
                arr(0).toInt.toHexString+arr(1).toInt.toHexString+arr(2).toInt.toHexString
            }
        }
    }

}
