package com;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.format.DateTimeFormatter;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/23/0023 10:19
 */
public class test {
    public static void main(String[] args) {

        String str = "1986-04-08 12:30";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        LocalDateTime dateTime = LocalDateTime.parse(str, formatter);


        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        LocalDateTime dateTime1 = LocalDateTime.of(1986, Month.APRIL, 8, 12, 30);
        String formattedDateTime = dateTime.format(formatter1); // "1986-04-08 12:30"

        System.out.println(convertDateStr("20210323"));


        System.out.println(new BigDecimal("1167425.0").divide(new BigDecimal("4600516.9"),3));

    }
    private static String convertDateStr(String origin){
        return origin.substring(0,4)+"-"+origin.substring(4,6)+"-"+origin.substring(6,8);
    }
}
