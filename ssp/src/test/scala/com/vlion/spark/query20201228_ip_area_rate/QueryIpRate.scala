package com.vlion.spark.query20201228_ip_area_rate

import net.ipip.ipdb.City
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit._

/**
 * @description:
 * @author: malichun
 * @time: 2020/12/28/0028 15:55
 *
 */
object QueryIpRate {
    //媒体请求
    //
    //ip没有找到地址,
    //只查到省份级别,没有城市级别
    //查到城市
    def main(args: Array[String]): Unit = {
//        testLocalIPV6()

        val conf = new SparkConf().setAppName("TEST_IP_CITY").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

        import spark.implicits._

        val ipDF = spark.sql(
            """
              |select
              |distinct(ip) as ip
              |from
              |ods.ods_media_req
              |where etl_date='2020-12-28' and etl_hour='12'
              |and ip is not null and size(split(ip,'\\.')) = 4
              |
              |""".stripMargin)
        val ipRdd = ipDF.rdd.map(r => r.getAs[String]("ip"))




    }

    /**
     * 0330的ip库统计
     * @param ipRdd
     */
    def computeIp0330(ipRdd:RDD[String]): Unit ={
        val resDF = ipRdd.mapPartitions(iter => {  //每个分区数据处理
            val configuration = new Configuration()
            val fs = FileSystem.get(configuration)
            val path = new Path("hdfs://www.bigdata02.com:8020/tmp/test/0330.ipdb");
            val in = fs.open(path);
            val db = new City(in)
            iter.map { ip =>

                val info = db.findInfo(ip, "CN")
                //                    info
                val country = info.getCountryName
                val province = info.getRegionName
                val city = info.getCityName
//                (country, province, city) match {
//                    case (x, _, _) if x == null || x.toUpperCase() == "NULL" || x == "" => (1, 0, 0, 0) //国家为空
//                    case (_, x, _) if x == null || x.toUpperCase() == "NULL" || x == "" => (1, 1, 0, 0) //省份为空
//                    case (_, _, x) if x == null || x.toUpperCase() == "NULL" || x == "" => (1, 1, 1, 0) //城市为空
//                    case _ => (1, 1, 1, 1) //精确到城市,城市不为空
//                }
                if(country == null || country.toUpperCase() == "NULL" || country.trim() == ""  ){
                    (1, 0, 0, 0)
                }else if (province == null || province.trim().toUpperCase() == "NULL" || province.trim() == ""){
                    (1, 1, 0, 0)
                }else if (city == null || city.trim().toUpperCase() == "NULL" || city.trim() == ""){
                    (1, 1, 1, 0)
                }else{
                    (1, 1, 1, 1)
                }
            }

        })

        resDF.reduce( (t1,t2) => ( t1._1 + t2._1, t1._2+ t2._2, t1._3+t2._3, t1._4+t2._4 ))

        (6474261,6474261,6474261,5546998)


    }

    def computeIp1228(ipRdd:RDD[String]): Unit ={
        val resDF = ipRdd.mapPartitions(iter => {  //每个分区数据处理
            val configuration = new Configuration()
            val fs = FileSystem.get(configuration)
            val path = new Path("hdfs://www.bigdata02.com:8020/tmp/test/0330.ipdb");
            val in = fs.open(path);
            val db = new City(in)
            iter.map { ip =>

                val info = db.findInfo(ip, "CN")
                //                    info
                val country = info.getCountryName
                val province = info.getRegionName
                val city = info.getCityName
                //                (country, province, city) match {
                //                    case (x, _, _) if x == null || x.toUpperCase() == "NULL" || x == "" => (1, 0, 0, 0) //国家为空
                //                    case (_, x, _) if x == null || x.toUpperCase() == "NULL" || x == "" => (1, 1, 0, 0) //省份为空
                //                    case (_, _, x) if x == null || x.toUpperCase() == "NULL" || x == "" => (1, 1, 1, 0) //城市为空
                //                    case _ => (1, 1, 1, 1) //精确到城市,城市不为空
                //                }
                if(country == null || country.toUpperCase() == "NULL" || country.trim() == ""  ){
                    (1, 0, 0, 0)
                }else if (province == null || province.trim().toUpperCase() == "NULL" || province.trim() == ""){
                    (1, 1, 0, 0)
                }else if (city == null || city.trim().toUpperCase() == "NULL" || city.trim() == ""){
                    (1, 1, 1, 0)
                }else{
                    (1, 1, 1, 1)
                }
            }

        })

        resDF.reduce( (t1,t2) => ( t1._1 + t2._1, t1._2+ t2._2, t1._3+t2._3, t1._4+t2._4 ))
    }

      def testLocalIPV6(): Unit ={
//        val configuration = new Configuration()
//        val fs = FileSystem.get(configuration)
//        val path = new Path("hdfs://www.bigdata02.com:8020/tmp/test/0330.ipdb");
//        val in = fs.open(path);
//        val db = new City(in)

          val list = List("10.200.149.14"
              ,"171.92.72.241"
              ,"171.92.72.241"
              ,"42.226.86.203"
              ,"60.191.32.118"
              ,"60.191.32.118"
              ,"117.136.47.77"
              ,"117.136.47.77"
              ,"117.136.47.77"
              ,"10.200.149.15"
              ,"60.191.32.118"
              ,"113.110.155.233")

        val db = new City("E:\\mlc\\微信文件\\WeChat Files\\mlc1240054918\\FileStorage\\File\\2020-11\\0330.ipdb")

          list.foreach { ip =>
              val info = db.findInfo(ip, "CN")
              //                    info
              val country = info.getCountryName
              val province = info.getRegionName
              val city = info.getCityName
              println("========" * 10)


              println(country)
              println(province)
              println(city)
              //全部,国家,省份,城市
              val res = if (country == null || country.toUpperCase() == "NULL" || country.trim() == "") {
                  (1, 0, 0, 0)
              } else if (province == null || province.trim().toUpperCase() == "NULL" || province.trim() == "") {
                  (1, 1, 0, 0)
              } else if (city == null || city.trim().toUpperCase() == "NULL" || city.trim() == "") {
                  (1, 1, 1, 0)
              } else {
                  (1, 1, 1, 1)
              }


              //          val res = (country, province, city) match {
              //              case (x, y, z) if x == null || x.toUpperCase() == "NULL" || (x.trim() equals  "" ) => (1, 0, 0, 0) //国家为空
              //              case (x, y, z) if y == null || y.toUpperCase() == "NULL" || (y.trim() equals "" )=> (1, 1, 0, 0) //省份为空
              //              case (x, y, z) if z == null || z.toUpperCase() == "NULL" || (z.trim() equals "" )=> (1, 1, 1, 0) //城市为空
              //              case _ => (1, 1, 1, 1) //精确到城市,城市不为空
              //          }

              println(res)
          }
    }
}
