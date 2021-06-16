import java.io.File

import org.apache.spark.sql.SparkSession

import scala.io.Source

object Test {
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//           .master("local[*]")
//      .appName("Ssp")
//      .config("hive.metastore.uris","trift://www.bigdata02.com:9083")
//      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
//      .config("hive.metastore.warehouse.dir","/user/hive/warehouse")
//      .config("spark.debug.maxToStringFields","100")
////      .enableHiveSupport()
//      .getOrCreate()
////    spark.sparkContext.setLogLevel("WARN")
//println("123")
////    sync.ImportData2
//
////    spark.sql("show databases").show
//
//    val sc=spark.sparkContext
//    val rdd=sc.textFile("/home/storm_new_dsp_resp_vlionserver-87_2020-07-02_20.log")
//    println(rdd.count())

var i = 10
    until(i==0){
      println(i)
      i -= 1
    }

println("===============")

    for(i<- 10 to 20 reverse){
      println(i)
    }

    for(i <- 1 to 10 if (i%2 ==0) ){

    }

    println("1===============")


    val j = for( i <- 1 to 10) yield i*i
    println(j)

    val sm = (x:Int) => 2*x

    
  }

  /**
   * 抽象控制来实现until
   * @param condition
   * @param block
   */
  def until(condition: => Boolean)(block: => Unit){
      if(!condition){
        block
        until(condition)(block)
      }
  }
}


class Person private(){

}


class Person2Student

object Person{
  def main(args: Array[String]): Unit = {
    //起个别名
    type p2s = Person2Student
    println(new p2s().getClass)
  }
}

package object abc{
  val A = "10"
  val foo:()=>Int = ()=> 1
}

package abc{
  class Person{
    def say()={
      println(A)
      foo
    }
  }
}


import scala.collection._
// 主构造函数私有. 将来只能在伴⽣对象中访问
class Marker private(val color: String) {
    println(s"Creating ${this}")
    override def toString = s"marker color $color"
}
object Marker {
    private val markers = mutable.Map(
        "red" -> new Marker("red"),
        "blue" -> new Marker("blue"),
        "yellow" -> new Marker("yellow"))
    def getMarker(color: String): Marker = markers.getOrElseUpdate(color, new Marker(color)
    )
    def main(args: Array[String]): Unit = {
        println(Marker getMarker "blue")
        println(Marker getMarker "blue")
        println(Marker getMarker "red")
        println(Marker getMarker "red")
        println(Marker getMarker "green")
    }
}



object ImplicitDemo01{
    def main(args: Array[String]): Unit = {


        implicit def file2RichFile(from:File):RichFile={
            new RichFile(from)
        }

        println("=============")
        //File 对象被隐式转换成了RichFile对象
        val r1  = new File(getClass.getClassLoader.getResource("a.txt").getPath)
        println(r1.read)

    }



}

class RichFile(val from:File){
    def read = Source.fromFile(from.getPath).mkString
}


object DateHelper{

    //////////////////////////////////////////////////////////////////
    //隐式类
    implicit class DateHelper(  day:Int){
        import java.time.LocalDate

        def days(when:String)={
            val today:LocalDate =LocalDate.now
            if(when == "ago"){
                today.minusDays(day)
            }else{
                today.plusDays(day)
            }
        }
    }

    //////////////////////////////////////////////////////////////////


    def main(args: Array[String]): Unit = {
        val ago = "ago"
        val from_now = "from_now"
        val past = 2 days ago   // 2 Int转为了对象,对象调用days方法,参数是2
        val future = 5 days from_now
        println(past)
        println(future)
    }

}



object ArrayDemo4 {
    def main(args: Array[String]): Unit = {
        var arr = Array(10, 21, 32, 4, 15, 46, 17)
        println(arr.sum) // 求和
        println(arr.max)
        println(arr.min)
        println(arr.mkString) // 把数组变成字符串
        println(arr.mkString(",")) // 把数组变成字符串, 元素之间⽤,隔开
        // 排序, 并返回⼀个排好序的数组 : 默认是升序
        val arrSorted: Array[Int] = arr.sorted
        println(arrSorted.mkString(","))
        // 降序
        var arrSorted1 = arr.sortWith(_ > _)
        println(arrSorted1.mkString(","))
    }

}

class Sample{
    val max = 10000
    def process(input:Int):Unit={
        input match {
            case `max` => println(s"You matched max")  //只能用小写,用大写会有问
        }
    }
}
object PatternTest {
    def main(args: Array[String]): Unit = {
        val sample = new Sample
        sample.process(1000)
    }
}











