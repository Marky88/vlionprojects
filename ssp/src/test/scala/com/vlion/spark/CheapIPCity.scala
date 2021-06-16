package com.vlion.spark

import java.io.{FileInputStream, ObjectInputStream}

import net.ipip.ipdb.{City, CityInfo, District, DistrictInfo}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * @description:
 * @author: malichun
 * @time: 2020/11/19/0019 15:17
 *
 */
object CheapIPCity {
    //返回CityInfo
    //country_name : 国家名字 （每周高级版及其以上版本包含）
    //region_name  : 省名字   （每周高级版及其以上版本包含）
    //city_name    : 城市名字 （每周高级版及其以上版本包含）
    //owner_domain : 所有者   （每周高级版及其以上版本包含）
    //isp_domain  : 运营商 （每周高级版与每日高级版及其以上版本包含）
    //latitude  :  纬度   （每日标准版及其以上版本包含）
    //longitude : 经度    （每日标准版及其以上版本包含）
    //timezone : 时区     （每日标准版及其以上版本包含）
    //utc_offset : UTC时区    （每日标准版及其以上版本包含）
    //china_admin_code : 中国行政区划代码 （每日标准版及其以上版本包含）
    //idd_code : 国家电话号码前缀 （每日标准版及其以上版本包含）
    //country_code : 国家2位代码  （每日标准版及其以上版本包含）
    //continent_code : 大洲代码   （每日标准版及其以上版本包含）
    //idc : IDC |  VPN   （每日专业版及其以上版本包含）
    //base_station : 基站 | WIFI （每日专业版及其以上版本包含）
    //country_code3 : 国家3位代码 （每日专业版及其以上版本包含）
    //european_union : 是否为欧盟成员国： 1 | 0 （每日专业版及其以上版本包含）
    //currency_code : 当前国家货币代码    （每日旗舰版及其以上版本包含）
    //currency_name : 当前国家货币名称    （每日旗舰版及其以上版本包含）
    //anycast : ANYCAST       （每日旗舰版及其以上版本包含）

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("TEST_IP_CITY").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

        import spark.implicits._

        //        val inputStream = new FileInputStream("E:\\mlc\\微信文件\\WeChat Files\\mlc1240054918\\FileStorage\\File\\2020-11\\0330.ipdb")

        val etlDate = "2020-11-19"
        val etlHour = "12"
        val reqDF = spark.sql(
            s"""
               |select
               |distinct -- 要去重,不要媒体了
               |k as id_type,
               |v as device_id,
               |ip
               |from
               |(
               |select
               |imei,idfa,android_id,mac,ip
               |from
               |ods.ods_media_req
               |where etl_date='${etlDate}' and etl_hour='${etlHour}'
               |and ip regexp '^\\\\d[\\\\d.]+\\\\d$$' --ip过滤
               |) t
               |lateral view explode(map('imei',imei,'idfa',idfa,'anid',android_id,'mac',mac)) m as k,v
               |where (v is not null and v !='' and v!='\\\\N' and (not v regexp '^[0:2-]*$$') and v !='没有权限' and v != 'unknown')
               |""".stripMargin
        )

        val resultDF: DataFrame = reqDF.rdd.map(row => (row.getAs[String]("id_type"), row.getAs[String]("device_id"), row.getAs[String]("ip")))
            .repartition(30)
            .mapPartitions(iter => {
                val configuration = new Configuration()
                val fs = FileSystem.get(configuration)
                val path = new Path("hdfs://www.bigdata02.com:8020/tmp/test/0330.ipdb");
                val in = fs.open(path);

                val db = new City(in)
                val res = iter.flatMap(tuple => {
                    val idType = tuple._1
                    val deviceId = tuple._2
                    val ip = tuple._3

                    val info = db.findInfo(ip, "CN")
                    //                    info
                    val country = info.getCountryName
                    val province = info.getRegionName
                    val city = info.getCityName
                    val buffer = ListBuffer[(String, String, String, String)]()
                    if (!(deviceId == null || deviceId.toUpperCase() == "NULL" || deviceId == "")) {
                        if (!(country == null || country.toUpperCase() == "NULL" || country == "")) {
                            buffer.append((idType, deviceId, "国家", country))
                        }

                        if (!(province == null || province.trim().toUpperCase() == "NULL" || province.trim() == "")) {
                            buffer.append((idType, deviceId, "省", province))
                        }
                        if (!(city == null || city.trim().toUpperCase() == "NULL" || city.trim() == "")) {
                            buffer.append((idType, deviceId, "市", city))
                        }
                    }
                    buffer.toList
                })
                in.close()
                fs.close()
                res
            })
            .map(t => (t, 1))
            .reduceByKey(_ + _) //设备id出现的次数
            .map {
                case ((idType, deviceId, areaType, areaName), cnt) =>
                    val range = cnt match {
                        case x if x <= 5 => x.toString
                        case x if x <= 8 => "6,7,8"
                        case x if x <= 10 => "9,10"
                        case _ => ">10"
                    }
                    ((areaType, areaName, idType, range), deviceId)
            } //取sample
            .combineByKey( //已经ByKey了,针对的是同一个key下面所有的数据
                (v: String) => (List(v), 1),
                (c: (List[String], Int), v: String) => {
                    if (c._1.size > 10) {
                        (c._1, c._2 + 1)
                    } else {
                        (c._1 :+ v, c._2 + 1)
                    }
                },
                (c1: (List[String], Int), c2: (List[String], Int)) => {
                    if (c1._1.size > 10) {
                        (c1._1, c1._2 + c2._2)
                    } else {
                        ((c1._1 ++: c2._1).take(10), c1._2 + c2._2)
                    }
                }
            )
            .map {
                case ((areaType, areaName, idType, range), (deviceIdSample, cnt)) =>
                    (areaType, areaName, idType, range, deviceIdSample.mkString(","), cnt)
            }
            .toDF("region_type", "region_name", "id_type", "range", "sample", "cnt")

        resultDF.coalesce(5)
            .write
            .format("jdbc")
            .option("url", "jdbc:mysql://172.16.197.73:3306/ssp_report?useUnicode=true&characterEncoding=utf8")
            .option("dbtable", "ip_id_analysis")
            .option("user", "root")
            .option("password", "123456")
            .mode("append")
            .save()


        sc.stop

        //        testCity()


    }


    def testCity() = {
        //地区市级
        try { // City类可用于IPDB格式的IPv4免费库，IPv4与IPv6的每周高级版、每日标准版、每日高级版、每日专业版、每日旗舰版
            //   /home/spark/ssp/data/output/cheap_data/test/0330.ipdb
            val inputStream = new FileInputStream("E:\\mlc\\微信文件\\WeChat Files\\mlc1240054918\\FileStorage\\File\\2020-11\\0330.ipdb")
            val db = new City(inputStream)
            // db.find(address, language) 返回索引数组
            //            println(db.find("1.1.1.1", "CN").toList )
            // db.findInfo(address, language) 返回 CityInfo 对象
            val info: CityInfo = db.findInfo("36.7.44.177", "CN")
            println(info)
            println("+" * 8)
            println(db.findInfo("1.12.13.1", "CN"))

        } catch {
            case e: Exception =>
                e.printStackTrace()
        }

        println("============================")
    }


}
