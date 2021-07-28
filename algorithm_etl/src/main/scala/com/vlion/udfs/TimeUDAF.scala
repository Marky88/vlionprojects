package com.vlion.udfs

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, StringType, StructField, StructType}

/**
 * @description:
 * @author: malichun
 * @time: 2021/7/22/0022 16:22
 *        第一次点击在本时间段内的秒数
 *        最后一次点击在本时间段内的秒数
 *        点击间隔最小时间
 *        点击间隔最大时间
 *        点击间隔平均时间
 */
class TimeUDAF(etlDate: String, etlHour: String) extends UserDefinedAggregateFunction {

    // 这个小时开始的秒数
    val startTimeSec = new SimpleDateFormat("yyyy-MM-dd HH").parse(s"$etlDate $etlHour").getTime / 1000

    // 如果只有天的话
    def this(etlDate:String){
        this(etlDate, "00")
    }

    // 输入时间戳(秒)
    override def inputSchema: StructType = StructType(StructField("time", LongType) :: Nil)

    //缓冲区数据类型
    override def bufferSchema: StructType = StructType(StructField("caltime", ArrayType(LongType)) :: Nil)

    // 输出的数据类型
    override def dataType: DataType = ArrayType(LongType)

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = Array[Long]()
    }

    //分区内合并
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getSeq[Long](0) :+ input.getLong(0)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getSeq[Long](0) ++: buffer2.getSeq[Long](0)
    }

    override def evaluate(buffer: Row): Any = {
        val arr = buffer.getSeq[Long](0).toArray
        val sortedArr = arr.sorted
        //  点击间隔最小时间
        //  点击间隔最大时间
        //  点击间隔平均时间
        if (sortedArr.size == 1) {
            Array(sortedArr.head - startTimeSec, // 第一次点击在本时间段内的秒数
                sortedArr(sortedArr.length - 1) - startTimeSec, // 最后一次点击在本时间段内的秒数
                -1,
                -1,
                -1
            )
        } else {
            //        点击间隔最小时间
            //        点击间隔最大时间
            //        点击间隔平均时间
            val tuples = sortedArr.slice(0, sortedArr.length - 1).zip(sortedArr.slice(1, sortedArr.length))
            val intervals = tuples.map { case (t1, t2) => t2 - t1 }
            val sumAndCount = intervals.map((_, 1)).reduceLeft((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2))

            Array(
                sortedArr.head - startTimeSec, // 第一次点击在本时间段内的秒数
                sortedArr(sortedArr.length - 1) - startTimeSec, // 最后一次点击在本时间段内的秒数
                intervals.min,
                intervals.max,
                sumAndCount._1 / sumAndCount._2
            )
        }
    }
}

