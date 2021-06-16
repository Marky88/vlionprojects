package com.vlion.spark.summer

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: malichun
 * @time: 2020/11/30/0030 15:39
 *
 */
trait TEngine  {
    var env:Any = null

    def start(engine:String = "spark")(op : => Unit):Unit={
        // TODO 启动资源
        if(engine == "spark"){
            val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
            env=new SparkContext(conf)
        }

        // 业务操作
        op

        //关闭
        if(engine == "spark"){
            env.asInstanceOf[SparkContext].stop()
        }



    }
}
