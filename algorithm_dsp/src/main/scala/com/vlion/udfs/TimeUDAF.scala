package com.vlion.udfs

import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, StructField, StructType}

/**
 * @description:
 * @author:
 * @time: 2021/12/7/0007 16:53
 *   第一次点击在本时间段内的秒数
 *   最后一次点击在本时间段内的秒数
 *   点击间隔最小时间
 *   点击间隔最大时间
 *   点击间隔平均时间
 *
 */
class TimeUDAF(etl_date:String,etl_hour:String)  extends UserDefinedAggregateFunction{
    val startTimeSec = new SimpleDateFormat("yyyy-MM-dd HH").parse(s"${etl_date} ${etl_hour}").getTime() / 1000   //毫秒转换成秒

    def this(etl_date: String){
        this(etl_date,"00")
    }

    //输入数据结构
    override def inputSchema: StructType = StructType(
        StructField("times",LongType)::Nil
    )

    //缓冲区数据结构
    override def bufferSchema: StructType = StructType(
        StructField("caltime",ArrayType(LongType))::Nil
    )

    //返回的数据结构
    override def dataType: DataType = ArrayType(LongType)

    //幂等操作 一般为true
    override def deterministic: Boolean = true

    //初始化缓冲区
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = Array[Long]()
    }

    //聚合函数 传入一条新的数据进行处理
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (input != null && input.size != 0 && input.getLong(0) != null  && input.getLong(0) != "" ){
            buffer(0) = buffer.getSeq[Long](0) :+ input.getLong(0)
        }
    }

    //分区间合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getSeq[Long](0) ++: buffer2.getSeq[Long](0)
    }

    //最终计算结果
    override def evaluate(buffer: Row): Any = {
        if (buffer.getSeq[Long](0) == null || buffer.getSeq[Long](0) == ""){
            Array(0L,0L,0L,0L,0L)
        }else{
            val array = buffer.getSeq[Long](0).toArray
            val sortedArr = array.sorted

            //  点击间隔最小时间
            //  点击间隔最大时间
            //  点击间隔平均时间
            if(sortedArr.size == 1){
                Array(
                   sortedArr.head - startTimeSec,
                    sortedArr(sortedArr.length - 1) - startTimeSec,
                    -1,
                    -1,
                    -1
                )
            }else{
                //        点击间隔最小时间
                //        点击间隔最大时间
                //        点击间隔平均时间
                val tuples = sortedArr.slice(0, sortedArr.length - 1).zip(sortedArr.slice(1, sortedArr.length))
                val sortedLog = tuples.map(x => (x._2 - x._1)).sorted
                Array(
                    sortedArr.head - startTimeSec,
                    sortedArr(sortedArr.length -1) - startTimeSec,
                    sortedLog.head,
                    sortedLog(sortedLog.length -1),
                    sortedLog.sum / sortedLog.length
                )
            }

        }











    }


}
