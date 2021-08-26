import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description:
 * @author: malichun
 * @time: 2021/8/18/0018 20:10
 *
 */
object Test {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .master("local[*]")
            .config("hive.metastore.uris","trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir","/user/hive/warehouse")
            .config("spark.debug.maxToStringFields","100")
            .enableHiveSupport()
            .getOrCreate()
        val sc = spark.sparkContext
//        spark.sql(
//            s"""
//               |select count(1) from ods.ods_ssp_imp where etl_date='2021-08-19'
//               |""".stripMargin) show

        val rdd = sc.makeRDD(1 to 10,2)
        val tuple = rdd.aggregate((0, 0))((c, e) => (c._1 + e, c._2 + 1), (c1, c2) => (c1._1 + c2._1, c1._2 + c2._2))
        println(tuple)

        rdd.mapPartitions { iter =>
            val tuple1 = ((0,0) /: iter){case (tuple,e) =>
                (tuple._1+e,tuple._2+1)
            }
            List(tuple1).iterator
        }.collect().reduceLeft((x,y) => (x._1+y._1,x._2+y._2))

        rdd.map((_,1)).reduce((x,y) => (x._1+y._1,x._2+y._2))





    }

}
