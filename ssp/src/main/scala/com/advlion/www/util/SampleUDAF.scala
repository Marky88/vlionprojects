package com.advlion.www.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType, StructField, StructType}

/**
 * @description:
 * @author: malichun
 * @time: 2020/11/12/0012 18:29
 *       聚合采样
 *
 */
class SampleUDAF extends UserDefinedAggregateFunction{
    val sampleSize = 10

    //输入数据类型
    override def inputSchema: StructType = StructType(
        StructField("col",ArrayType(StringType) ):: Nil
    )

    //缓存的数据类型
    override def bufferSchema: StructType ={
        StructType(StructField("cols",ArrayType(StringType)) :: Nil)
    }

    // 输出的数据类型
    override def dataType: DataType = StringType
    //相同的输入时候有相同的输出
    override def deterministic: Boolean = true

    //给存储数据初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = Array[String]()
    }

    //分区内合并 Array[String]()
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val arr = buffer.getAs[Array[String]](0)
        if( arr.length < sampleSize){
            arr :+ input.getString(0)
        }

    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

    override def evaluate(buffer: Row): Any = ???
}
