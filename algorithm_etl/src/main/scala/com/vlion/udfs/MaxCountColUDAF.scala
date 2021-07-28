package com.vlion.udfs

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}


/**
 * @description:
 * @author: malichun
 * @time: 2021/7/22/0022 13:54
 *
 *        找出出现次数最大的一个
 *
 */
class MaxCountColUDAF extends UserDefinedAggregateFunction {
    // 输入的类型
    override def inputSchema: StructType = {
        StructType(StructField("col", StringType) :: Nil)
    }

    // 聚合缓冲区数据类型
    override def bufferSchema: StructType = {
        StructType(StructField("col_count", MapType(StringType, LongType)) :: Nil)
    }

    // 输出数据类型
    override def dataType: DataType = StringType //返回值类型


    override def deterministic: Boolean = true

    // 给存储数据初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = Map[String, Long]()
    }

    // 分区内合并 Map[col,次数]
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val colName = input.getString(0)
        if(colName!=null && colName!="") {
            val map: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
            buffer(0) = map + (colName -> (map.getOrElse(colName, 0L) + 1L))
        }

    }

    // 分区间合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val map1 = buffer1.getAs[Map[String, Long]](0)
        val map2 = buffer2.getAs[Map[String, Long]](0)

        // 把map1的键值对与map2中的累积, 最后赋值给buffer1
        buffer1(0) = map1.foldLeft(map2) {
            case (map, (k, v)) =>
                map + (k -> (map.getOrElse(k, 0L) + v))
        }
    }

    // 最终输出出现最高的
    override def evaluate(buffer: Row): Any = {
        val colCountMap = buffer.getAs[Map[String, Long]](0)
        if(colCountMap nonEmpty){
            val res = colCountMap.toList.sortWith((t1, t2) => {
                t1._2 > t2._2
            }).head._1

            // 输出最高的
            res
        }else{
            ""
        }
    }
}
