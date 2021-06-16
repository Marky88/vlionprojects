package com.vlion.spark

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.util.Progressable
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.spark.input.PortableDataStream

import scala.util.Try
import java.nio.charset._

import net.ipip.ipdb.City
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @description:
 * @author: malichun
 * @time: 2020/11/26/0026 10:41
 *
 *
 *        帮忙提取一下历史数据，6月1日一整天
 *        24555、24553、24557这3个tagid的101数据，生成3个文件给我
 */
object SSPGet101File {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setAppName("SSPGet101File").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val spark = SparkSession.builder().getOrCreate()

        import spark.implicits._

        //        val inputStream = new FileInputStream("E:\\mlc\\微信文件\\WeChat Files\\mlc1240054918\\FileStorage\\File\\2020-11\\0330.ipdb")

        val outputBasePath = "/tmp/test/20201126/out"

        val rdd = sc.textFile("/tmp/test/20201126/input")

//        val rdd = sc.binaryFiles("somePath").flatMapValues(x =>
//            extractFiles(x).toOption).mapValues(_.map(decode()))

        //def mapPartitionsWithIndex[U: ClassTag](
        //      f: (Int, Iterator[T]) => Iterator[U],
        //      preservesPartitioning: Boolean = false): RDD[U]

        rdd.map(line => (line.split("\t")(3),line))
            .filter(t => {t._1 == "24555" || t._1 == "24553" || t._1 == "24557"})
            .partitionBy(new MyPartitioner)
            .mapPartitionsWithIndex{ case (tagId,iter) =>
                val path = new Path(outputBasePath +"/"+tagId+".txt")
                val configuration = new Configuration()
                val fs = FileSystem.get(configuration)
                val fsDataOutputStream: FSDataOutputStream = fs.create(path)
                val br = new BufferedWriter( new OutputStreamWriter( fsDataOutputStream, "UTF-8" ) )
                iter.foreach(line => {
                    br.write(line._2)
                    br.newLine()
                    br.flush()
                })
                br.close()
                fsDataOutputStream.close()
                List(tagId).toIterator
            }.collect


        //
//        val rdd24553 = sc.textFile("/tmp/test/20201126/cheap/24553.txt")
//        computeIPAreaIPFront(rdd24553,"/tmp/test/20201126/cheap/24553_merge")
//
//
//        val rdd24555 = sc.textFile("/tmp/test/20201126/cheap/24555.txt")
//        computeIPAreaIPFront(rdd24555,"/tmp/test/20201126/cheap/24555_merge")
//
//
//        val rdd24557 = sc.textFile("/tmp/test/20201126/cheap/24557.txt")
//        computeIPAreaIPFront(rdd24557,"/tmp/test/20201126/cheap/24557_merge")


        //请求全部
        val rddALL = sc.textFile("/tmp/test/20201126/input")
        val tempRDD=computeIPAreaIPFront(rddALL,spark)
        import spark.implicits._
        val data=tempRDD.map(line => {
            val arr = line.split("\t")
            Row(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8),arr(9),arr(10),arr(11),arr(12),arr(13),arr(14),arr(15),arr(16),arr(17),arr(18),arr(19),arr(20),arr(21),arr(22)
                ,arr(23),arr(24),arr(25),arr(26),arr(27),arr(28),arr(29),arr(30),
                arr(31),arr(32),arr(33),arr(34),arr(35),arr(36),arr(37),arr(38),arr(39),arr(40),arr(41),arr(42),arr(43),arr(44),arr(45))
        })

        val schema = StructType(
            StructField("logtype",StringType)::
                StructField("time",StringType              )::
                StructField ("app_id",StringType            )::
                StructField ("adsolt_id",StringType         )::
                StructField("adsolt_type",StringType       )::
                StructField("request_id",StringType        )::
                StructField("ip",StringType                )::
                StructField("imei",StringType              )::
                StructField("mac",StringType               )::
                StructField("android_id",StringType        )::
                StructField("idfa",StringType              )::
                StructField("os",StringType                )::
                StructField("carrier",StringType           )::
                StructField("network",StringType           )::
                StructField("device_type",StringType       )::
                StructField("floor_price",StringType       )::
                StructField("user_agent",StringType        )::
                StructField("cookie",StringType            )::
                StructField("error_code",StringType        )::
                StructField("encryption_phone",StringType  )::
                StructField("sex",StringType               )::
                StructField("age",StringType               )::
                StructField("education",StringType         )::
                StructField("marriage",StringType          )::
                StructField("job",StringType               )::
                StructField("producer",StringType          )::
                StructField("model",StringType             )::
                StructField("city_id",StringType           )::
                StructField("is_sdk",StringType            )::
                StructField("adsolt_width",StringType      )::
                StructField("adsolt_height",StringType     )::
                StructField("osv",StringType               )::
                StructField("appv",StringType              )::
                StructField("mgtv_req_id",StringType       )::
                StructField("mgtv_flow",StringType         )::
                StructField("mgtv_adslot_id",StringType    )::
                StructField("oaid",StringType              )::
                StructField("ip3",StringType               )::
                StructField("country",StringType           )::
                StructField("province",StringType          )::
                StructField("city",StringType              )::
                StructField("exists_ip",StringType         )::
                StructField("exists_ip3",StringType        )::
                StructField("exists_idfa",StringType       )::
                StructField("exists_imei",StringType       )::
                StructField("exists_anid",StringType       )::
                Nil
        )

        spark.createDataFrame(data,schema).createOrReplaceTempView("temp_req")
//        toDF("logtype","time","app_id","adsolt_id","adsolt_type","request_id",
//            "ip","imei","mac","android_id","idfa","os","carrier","network","device_type",
//            "floor_price","user_agent","cookie","error_code","encryption_phone","sex",
//            "age","education","marriage","job","producer","model","city_id","is_sdk",
//            "adsolt_width","adsolt_height","osv","appv","mgtv_req_id","mgtv_flow","mgtv_adslot_id",
//            "oaid","ip3","country","province","city","exists_ip","exists_ip3","exists_idfa","exists_imei","exists_anid")
//            .createOrReplaceTempView("temp_req")

        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("set hive.exec.dynamic.partition=true")
        spark.sql("insert overwrite table test.ods_media_req_2 partition(app_id,adsolt_id) select * from temp_req")
    }

    /**
     * 统计获取ip段,ip区域,和redis ct:ip比较,ct:ip3比较,ct:idfa,ct:imei
     * @param rdd
     * @return line + ip3段,国家,省份,城市,   是否在redis:ip中(1存在,0存在),   ct:ip3比较,      ct:idfa,   ct:imei,   ct:anid
     */
    def computeIPAreaIPFront(rdd:RDD[String],spark:SparkSession)={

        val tempRDD=rdd.repartition(500)
            .mapPartitions(iter => {
                val return_ = ListBuffer[String]()

            //确认城市
                val configuration = new Configuration()
                val fs = FileSystem.get(configuration)
                val path = new Path("hdfs://www.bigdata02.com:8020/tmp/test/0330.ipdb");
                val in = fs.open(path);
            //确认是否在redis中,每个partition初始化
                val client = new Jedis("172.16.189.215", 6379)

                val db = new City(in)

                val patterImei = "^0*$".r

                val stringBuilder = new StringBuilder()
                val res = iter.foreach(line => {

                    stringBuilder.append(line)
                    val arr = line.split("\t")

                    val ip = arr(6)
                    val idfa = arr(10)
                    val imei = arr(7)
                    val anid = arr(9)

                    val info = db.findInfo(ip, "CN")
                    //                    info
                    val country = info.getCountryName
                    val province = info.getRegionName
                    val city = info.getCityName

                    val ip3 = getIpFront(ip)
                    stringBuilder.append("\t").append(ip3)  //ip3

                    if (!(country == null || country.toUpperCase() == "NULL" || country == "")) {
                        stringBuilder.append("\t").append(country)
                    }else{
                        stringBuilder.append("\t").append("")
                    }


                    if (!(province == null || province.trim().toUpperCase() == "NULL" || province.trim() == "")) {
                        stringBuilder.append("\t").append(province)
                    }else{
                        stringBuilder.append("\t").append("")
                    }
                    if (!(city == null || city.trim().toUpperCase() == "NULL" || city.trim() == "")) {
                        stringBuilder.append("\t").append(city)
                    }else{
                        stringBuilder.append("\t").append("")
                    }

                    //初始是否在redis中...
                    //ip
                    val isIpExists = client.exists("ct:ip:" + ip)
                    stringBuilder.append("\t").append(if(isIpExists) "1" else "0")
                    //ip3
                    val isIp3Exists = client.exists("ct:ip3:" + ip3)
                    stringBuilder.append("\t").append(if(isIp3Exists) "1" else "0")
                    //idfa
                    if(idfa == null || idfa.trim == ""){
                        stringBuilder.append("\t").append("")
                    }else{
                        val isIdfaExists = client.exists("ct:idfa"+idfa)
                        stringBuilder.append("\t").append(if(isIdfaExists) "1" else "0")
                    }
                    //imei
                    if(imei == null || imei.trim == "" || patterImei.findFirstIn(imei).isEmpty ){
                        stringBuilder.append("\t").append("")
                    }else{
                        val isImeiExists = client.exists("ct:imei"+imei)
                        stringBuilder.append("\t").append(if(isImeiExists) "1" else "0")
                    }
                    //anid
                    if(anid == null || anid.trim == ""){
                        stringBuilder.append("\t").append("")
                    }else{
                        val isAnidExists = client.exists("ct:anid"+anid)
                        stringBuilder.append("\t").append(if(isAnidExists) "1" else "0")
                    }


                    return_.append(stringBuilder.toString())
                    stringBuilder.clear()
                })
                in.close()
                fs.close()
                if (client != null) client.close()
                return_.toIterator
            })
//            .saveAsTextFile(outPath)
        tempRDD


    }


    def getIpFront(ip:String):String={
        if(ip == null) return ""
        val arr = ip.split("\\.")
        if(arr.length != 4) return ""
        arr(0).toInt.toHexString+arr(1).toInt.toHexString+arr(2).toInt.toHexString
    }



    def extractFiles(ps: PortableDataStream, n: Int = 1024) = Try {
        val tar = new TarArchiveInputStream(new GzipCompressorInputStream(ps.open))
        Stream.continually(Option(tar.getNextTarEntry))
            // Read until next exntry is null
            .takeWhile(_.isDefined).flatten
            // Drop directories
            .filter(!_.isDirectory)
            .map(e => {
                Stream.continually {
                    // Read n bytes
                    val buffer = Array.fill[Byte](n)(-1)
                    val i = tar.read(buffer, 0, n)
                    (i, buffer.take(i))
                }
                    // Take as long as we've read something
                    .takeWhile(_._1 > 0).flatMap(_._2)
                    .toArray})
            .toArray
    }

    def decode(charset: Charset = StandardCharsets.UTF_8)(bytes: Array[Byte]) =
        new String(bytes, StandardCharsets.UTF_8)




}

class MyPartitioner() extends Partitioner {
    override def numPartitions: Int = 6


    override def getPartition(key: Any): Int = {
        val randomInt = scala.util.Random.nextInt(2) //生成0-1整数
        if(key.toString == "24555"){
            0+randomInt
        }else if(key.toString == "24553"){
            2+randomInt
        }else ( //24557
            4+randomInt
        )
    }
}


