import com.vlion.udfs.{MaxCountColUDAF, TimeUDAF}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author:
 * @time: 2021/12/7/0007 9:47
 *
 */
object udafTest {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .appName("algorithm_dsp")
            .master("local[2]")
/*            .config("hive.metastore.uris", "trift://www.bigdata02.com:9083")
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")
            .config("spark.debug.maxToStringFields", "100")*/
           // .enableHiveSupport()
            .getOrCreate()

        //val etl_date = args(0)

        val input = spark.read.textFile("data/student.txt")
        input.toDF().createOrReplaceTempView("student")

        spark.udf.register("maxcolcount",new MaxCountColUDAF)

        val frame = spark.sql(
            """
              |
              |select
              |  maxcolcount(value) as max_col
              |from
              |  student
              |""".stripMargin
        )

        frame.show()

        val arr: Array[Long] =  new Array[Long](3)
        arr(0) =2L
        arr(1) =7L
        arr(2) =4L


        val sorted = arr.sorted
        sorted.map(x => println(x))


        println("arr的长度"+arr.length)
        println("arr的结果"+arr.sum)


        val arr2 = new Array[Long](3)
        arr2(0) = 3L
        arr2(1) = 5L
        arr2(2) = 6L

            val tuples: Array[(Long, Long)] = arr.slice(0, arr.length ).zip(arr.slice(1, arr.length))
            tuples.map(x => println(x))


        println("==================")

        val input2 = spark.read.textFile("data/sort.txt")
        input2.createOrReplaceTempView("sortDemo")

        spark.udf.register("sorted",new TimeUDAF("2021-12-08","19"))

        val frame1 = spark.sql(
            """
              |select
              | sorted(value)
              |from
              |sortDemo
              |
              |""".stripMargin
        )

        frame1.write.json("data/udafa")


    }

}
