package com.vlion.kudu

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import scala.collection.JavaConverters._

/**
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @author malichun
 * @date 2020/7/14 001417:48
 */
object DataFrameKudu {
    def main(args: Array[String]): Unit = {
        //构建 SparkConf 对象
        val sparkConf: SparkConf = new SparkConf().setAppName("DataFrameKudu").setMaster("local[2]")
        //构建 SparkSession 对象
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        //获取 SparkContext 对象
        val sc: SparkContext = sparkSession.sparkContext
        sc.setLogLevel("warn")
        //指定 kudu 的 master 地址
        val kuduMaster = "node1:7051,node2:7051,node3:7051"
        //构建 KuduContext 对象
        val kuduContext = new KuduContext(kuduMaster, sc)
        //定义表名
        val tableName = "people"
        //1、创建表
        createTable(kuduContext, tableName)
        //2、插入数据到表中
        insertData2table(sparkSession, sc, kuduContext, tableName)
    }

    /**
     * 创建表
     *
     * @param kuduContext
     * @param tableName
     */
    private def createTable(kuduContext: KuduContext, tableName: String): Unit = {
        //定义表的 schema
        val schema = StructType(
            StructField("id", IntegerType, false) ::
                StructField("name", StringType, false) ::
                StructField("age", IntegerType, false) :: Nil
        )
        //定义表的主键
        val tablePrimaryKey = List("id")
        //定义表的选项配置
        val options = new CreateTableOptions
        options.setRangePartitionColumns(List("id").asJava)
        options.setNumReplicas(1)
        //创建表
        if (!kuduContext.tableExists(tableName)) {
            kuduContext.createTable(tableName, schema, tablePrimaryKey, options)
        }
    }

    /**
     * 插入数据到表中
     * * @param sparkSession
     * * @param sc
     * * @param kuduContext
     * * @param tableName
     **/
    private def insertData2table(sparkSession: SparkSession, sc: SparkContext, kuduContext: KuduContext, tableName: String): Unit = {
        //准备数据
        val data = List(People(1, "zhangsan", 20), People(2, "lisi", 30), People(3, "wangwu", 40))
        val peopleRDD: RDD[People] = sc.parallelize(data)
        import sparkSession.implicits._
        val peopleDF: DataFrame = peopleRDD.toDF
        kuduContext.insertRows(peopleDF, tableName)

    }

    /**
     * 删除表的数据
     *
     * @param sparkSession
     * @param sc
     * @param kuduMaster
     * @param kuduContext
     * @param tableName
     */
    private def deleteData(sparkSession: SparkSession, sc: SparkContext, kuduMaster: String, kuduContext: KuduContext, tableName: String): Unit = {
        //定义一个 map 集合，封装 kudu 的相关信息
        val options = Map(
            "kudu.master" -> kuduMaster,
            "kudu.table" -> tableName
        )
        import sparkSession.implicits._
        val data = List(People(1, "zhangsan", 20), People(2, "lisi", 30), People(3, "wangwu", 40))
        val dataFrame: DataFrame = sc.parallelize(data).toDF
        dataFrame.createTempView("temp")
        //获取年龄大于 30 的所有用户 id
        val result: DataFrame = sparkSession.sql("select id from temp where age >30")
        //删除对应的数据，这里必须要是主键字段
        kuduContext.deleteRows(result, tableName)
    }


    /**
     * 更新数据--添加数据
     *
     * @param sc
     * @param kuduMaster
     * @param kuduContext
     * @param tableName
     */
    private def UpsertData(sparkSession: SparkSession, sc: SparkContext, kuduMaster: String,
                           kuduContext: KuduContext, tableName: String): Unit = {
        //更新表中的数据
        //定义一个 map 集合，封装 kudu 的相关信息
        val options = Map(
            "kudu.master" -> kuduMaster,
            "kudu.table" -> tableName
        )
        import sparkSession.implicits._
        val data = List(People(1, "zhangsan", 50), People(5, "tom", 30))
        val dataFrame: DataFrame = sc.parallelize(data).toDF
        //如果存在就是更新，否则就是插入
        kuduContext.upsertRows(dataFrame, tableName)
    }


    /**
     * 更新数据
     *
     * @param sparkSession
     * @param sc
     * @param kuduMaster
     * @param kuduContext
     * @param tableName
     */
    private def updateData(sparkSession: SparkSession, sc: SparkContext, kuduMaster: String,
                           kuduContext: KuduContext, tableName: String): Unit = {
        //定义一个 map 集合，封装 kudu 的相关信息
        val options = Map(
            "kudu.master" -> kuduMaster,
            "kudu.table" -> tableName
        )
        import sparkSession.implicits._
        val data = List(People(1, "zhangsan", 60), People(6, "tom", 30))
        val dataFrame: DataFrame = sc.parallelize(data).toDF
        //如果存在就是更新，否则就是报错
        kuduContext.updateRows(dataFrame, tableName)
    }


    /**
     * 使用 DataFrameApi 读取 kudu 表中的数据
     *
     * @param spark
     * @param kuduMaster
     * @param tableName
     */
    private def getTableData(spark: SparkSession, kuduMaster: String, tableName: String): Unit = {
        //定义 map 集合，封装 kudu 的 master 地址和要读取的表名
        val options = Map(
            "kudu.master" -> kuduMaster,
            "kudu.table" -> tableName
        )
        spark.read.options(options).format("kudu").load  //版本有问题

    }

    /**
     * DataFrame api 写数据到 kudu 表
     * @param sparkSession
     * @param sc
     * @param kuduMaster
     * @param tableName
     */
    private def dataFrame2kudu(sparkSession: SparkSession, sc: SparkContext, kuduMaster:
    String, tableName: String): Unit = {
        //定义 map 集合，封装 kudu 的 master 地址和要读取的表名
        val options = Map(
            "kudu.master" -> kuduMaster,
            "kudu.table" -> tableName
        )
        val data = List(People(7, "jim", 30), People(8, "xiaoming", 40))
        import sparkSession.implicits._
        val dataFrame: DataFrame = sc.parallelize(data).toDF
        //把 dataFrame 结果写入到 kudu 表中 ,目前只支持 append 追加
//        dataFrame.write.options(options).mode("append").format("kudu")  // 有问题
        dataFrame.write
            .options(options)
            .mode("append")
            .format("kudu").save
        //查看结果
        //导包
        import org.apache.kudu.spark.kudu._
        //加载表的数据，导包调用 kudu 方法，转换为 dataFrame，最后在使用 show 方法显示结果
        sparkSession.read.options(options).kudu.show()
    }


    /**
     * 使用 sparksql 操作 kudu 表
     * @param sparkSession
     * @param sc
     * @param kuduMaster
     * @param tableName
     */
    private  def  SparkSql2Kudu(sparkSession:  SparkSession,  sc:
    SparkContext, kuduMaster: String, tableName: String): Unit = {
        //定义 map 集合，封装 kudu 的 master 地址和表名
        val options = Map(
            "kudu.master" -> kuduMaster,
            "kudu.table" -> tableName
        )
        val data = List(People(10, "小张", 30), People(11, "小王", 40))
        import sparkSession.implicits._
        val dataFrame: DataFrame = sc.parallelize(data).toDF
        //把 dataFrame 注册成一张表
        dataFrame.createTempView("temp1")
        //获取 kudu 表中的数据，然后注册成一张表
        sparkSession.read.options(options).format("kudu").load.createTempView("temp2")
        //使用 sparkSQL 的 insert 操作插入数据
        sparkSession.sql("insert into table temp2 select * from temp1")
        sparkSession.sql("select * from temp2 where age >30").show()
    }



}

case class People(id: Int, name: String, age: Int)