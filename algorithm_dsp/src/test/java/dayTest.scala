import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * @description:
 * @author:
 * @time: 2021/12/10/0010 18:43
 *
 */
object dayTest {
    def main(args: Array[String]): Unit = {

       /* val etl_date = "2021-11-08 02:00:00"

        val timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(etl_date)
        println(timestamp)*/


        val etl_date = "2021-12-11"
        val calDay = 3
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val date = dateFormat.parse(etl_date)
        val calendar = Calendar.getInstance()
        calendar.setTime(date)
        calendar.add(Calendar.DATE,-(calDay-1))
        val headDay = dateFormat.format(calendar.getTime())
        println(headDay)



    }

}
