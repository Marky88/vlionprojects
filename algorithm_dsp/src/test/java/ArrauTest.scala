import com.vlion.utils.Bundles

/**
 * @description:
 * @author:
 * @time: 2021/12/9/0009 17:15
 *
 */
object ArrauTest {
    def main(args: Array[String]): Unit = {
        val bundles = Bundles.bundles
        val string: String = bundles.mkString(",")
        println(string)


    }

}
