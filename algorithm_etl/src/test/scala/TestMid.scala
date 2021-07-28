import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author: malichun
 * @time: 2021/7/23/0023 9:21
 *
 */
object TestMid {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().config("master", "local[*]").getOrCreate();

        val sc = spark.sparkContext

        val rdd = sc.makeRDD(1 to 100)

        val max = rdd.max()
        val min = rdd.min()
        val count = rdd.count()
        val midIndex = count / 2


        val rdd2 = rdd.map((_, 1)).partitionBy(new MyPartitioner(10, min, max))

        // 计算每个分区的数目
        val sortedNum = rdd2
            .mapPartitionsWithIndex((index, iter) => {
                val count = iter.foldLeft(0)((sum, _) => {
                    sum + 1
                })
                List((index, count)).iterator
            }).collect.sorted

        // 计算中位数在第几个分区
        val usefulPartition = sortedNum.foldLeft((0, 0)) { case ((sum, index), (partitionNo, nums)) =>
            if ((sum + nums) <= midIndex) {
                (sum + nums, partitionNo)
            } else {
                (sum, index)
            }
        }

        rdd2.mapPartitionsWithIndex((index,iter) =>{
            if(index == usefulPartition._2){
                val partitionIndex = midIndex.toInt - usefulPartition._1
                val sorted = iter.map(_._1).toList.sorted
                List(sorted(partitionIndex)).toIterator
            }else{
                List.empty[Int].iterator
            }
        }).collect()

    }
}

class MyPartitioner(numPars: Int, start: Int, end: Int) extends Partitioner {
    val perPartitionsNum = (end - start + 1) / numPars // 每个分区的范围


    override def numPartitions = numPars

    override def getPartition(key: Any) = {
        key match {
            case x: Int => (x - start) / perPartitionsNum
            case _ => 0
        }
    }
}
