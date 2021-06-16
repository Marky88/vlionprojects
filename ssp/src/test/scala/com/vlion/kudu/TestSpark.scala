package com.vlion.kudu

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.JavaConverters._

/**
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @author malichun
 * @date 2020/7/14 001413:45
 */
class TestSpark {
    def main(args: Array[String]): Unit = {
        //构建 sparkConf 对象
        val sparkConf: SparkConf = new SparkConf().setAppName("SparkKuduTest").setMaster("local[2]")
        //构建 SparkSession 对象
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        //获取 sparkContext 对象
        val sc: SparkContext = sparkSession.sparkContext
        sc.setLogLevel("warn")

        //构建 KuduContext 对象
        val kuduContext = new KuduContext("node1:7051,node2:7051,node3:7051", sc)
        //1.创建表操作
        createTable(kuduContext)



        val tableName="impala::dm.ssp_report_test"
        //使用 kuduContext 对象调用 kuduRDD 方法，需要 sparkContext 对象，表名，想要的字段名称
        val  kuduRDD:  RDD[Row]  = kuduContext.kuduRDD(sc,tableName,Seq("name","age"))
        //操作该 rdd 打印输出
        val result: RDD[(String, Int)] = kuduRDD.map {
            case Row(name: String, age: Int) => (name, age)
        }
        result.foreach(println)


    }

    /**
     * 创建表
     *
     * @param kuduContext
     * @return
     */
    private def createTable(kuduContext: KuduContext) = {
        //1.1 定义表名
        val tableName = "spark_kudu"
        //1.2 定义表的 schema
        val schema = StructType(
            StructField("userId", StringType, false) ::
                StructField("name", StringType, false) ::
                StructField("age", IntegerType, false) ::
                StructField("sex", StringType, false) :: Nil)
        //1.3 定义表的主键
        val primaryKey = Seq("userId")
        //1.4 定义分区的 schema
        val options = new CreateTableOptions
        //设置分区
        options.setRangePartitionColumns(List("userId").asJava)
        //设置副本
        options.setNumReplicas(1)
        //1.5 创建表
        if (!kuduContext.tableExists(tableName)) {
            kuduContext.createTable(tableName, schema, primaryKey, options)
        }
    }






}

