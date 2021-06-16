package com.advlion.www.dao

import org.apache.spark.sql.DataFrame
import redis.clients.jedis.Jedis

object RedisDAO {
  val redisHost = "172.16.189.196"
  val redisPort = 6379
  //    val redisPassword = ""
  val expireDays = 1
  def writeToRedis(df:DataFrame,title:String): Unit ={
    df.repartition(5).foreachPartition(
      rows => {
        val rc = new Jedis(redisHost, redisPort)
        //        rc.auth(redisPassword)
        rc.select(1)
        val pipe = rc.pipelined
        rows.foreach(
          r => {
            val redisKey = "ct:"+ title + ":" + r.getAs[String](title)
            //            val redisValue = r.getAs[String]("value")
            val redisValue = "1"
            pipe.set(redisKey, redisValue)
            pipe.expire(redisKey, expireDays * 3600 * 24)
          })

        pipe.sync()
      })
  }

  //20201110新增
  def writeToRedis2(redisHost:String, redisPort:Int, redisDB :Int)(implicit df:DataFrame,numPartition:Int) = {
    df.repartition(numPartition).foreachPartition(rows => {
      val rc = new Jedis(redisHost, redisPort)
      rc.select(redisDB)
      val pipe = rc.pipelined
      rows.foreach(
        r => {
          val deviceType= r.getAs[String]("device_type")
          var id = r.getAs[String]("v")
          val redisKey = "ct:"+ deviceType + ":" + id
          //            val redisValue = r.getAs[String]("value")
          val redisValue = "1"
          pipe.set(redisKey, redisValue)
          pipe.expire(redisKey,  3600 * 2 ) //过期两小时
        })

      pipe.sync()

    })
  }




}
