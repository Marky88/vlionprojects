package com.vlion.udfs



import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}


/**
 * @description:
 * @author:
 * @time: 2021/12/7/0007 10:01
 *       求出该列出现最多的次数
 *
 */
class MaxCountColUDAF extends UserDefinedAggregateFunction{
    //输入数据结构
    override def inputSchema: StructType = StructType(
        StructField("col",StringType)::Nil
    )

    //缓冲数据结构
    override def bufferSchema: StructType = StructType(
        StructField("col_count",MapType(StringType,LongType))::Nil
    )

    //返回数据结构
    override def dataType: DataType = StringType

    //是否幂等
    override def deterministic: Boolean = true

    //初始化缓冲区
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = Map[String,Long]()
    }

    //聚合函数传入一条新数据进行处理
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val colName = input.getString(0)
        if(colName != null && colName !=""){
            val map = buffer.getAs[Map[String, Long]](0)
            val value = map.getOrElse(colName, 0L) + 1L
            buffer(0) = map + (colName -> value)
        }
    }

    //合并聚合函数缓冲区  分区间合并  spark多个节点计算后，需要进行合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val map1 = buffer1.getAs[Map[String, Long]](0)
        val map2 = buffer2.getAs[Map[String, Long]](0)

        buffer1(0) = map2.foldLeft[Map[String, Long]](map1) {
            case (map, (k, v)) => {
                val value = map.getOrElse(k, 0L)
                map + (k -> (value + v))
            }
        }


    }

    // 计算最终结果
    override def evaluate(buffer: Row): Any = {

        val colcountmap = buffer.getAs[Map[String, Long]](0)
        if (colcountmap.nonEmpty) {
            val key = colcountmap
                .toList.sortWith((t1, t2) => t1._2 > t2._2).head._1

            key
        }else{
            ""
        }

    }




}
