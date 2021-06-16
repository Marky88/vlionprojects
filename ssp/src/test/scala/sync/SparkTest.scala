package sync

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/2/3/0003 17:49
 *
 */
object SparkTest {
    def main(args: Array[String]): Unit = {
        // TODO 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()


        val rdd = spark.sparkContext.makeRDD(List(("zhansan",10),("lisi",20),("wangwu",18)))

        import spark.implicits._

        val df = rdd.toDF("name","age")
        df.select('name,'age as 'my_ag ).show()

        df.filter('age > 15).show()

        df.groupBy("age").count.show

        val list = List(Person("zhansan",30),Person("list",40))
        list.toDS()

    }

}
case class Person(name:String, age:Int)
