/**
 * @description:
 * @author:
 * @time: 2021/12/9/0009 16:12
 *
 */
object ecodeTest {
    def main(args: Array[String]): Unit = {
        val str = "Mozilla%2F5.0%20(Linux%3B%20Android%2010%3B%20M2006C3LII%20Build%2FQP1A.190711.020%3B%20wv)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Version%2F4.0%20Chrome%2F87.0.4280.101%20Mobile%20Safari%2F537.36"

        val ecode = java.net.URLEncoder.encode(str)
        println("编码:"+ecode)

        val decode = java.net.URLDecoder.decode(ecode)
        println("解码:" + decode)


    }

}
