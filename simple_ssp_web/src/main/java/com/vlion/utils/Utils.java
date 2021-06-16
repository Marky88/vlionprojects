package com.vlion.utils;

import sun.misc.BASE64Encoder;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.LocalDateTime;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/23/0023 15:18
 */
public class Utils {
    public static String getObjectValueString(Object o){
        return o == null ? "":o.toString();
    }

    public static double getObjectValueDouble(Object o){
        return o == null ? 0.0: Double.parseDouble(o.toString());
    }

    public static int getObjectValueInteger(Object o){
        return o == null ? 0:(int)Double.parseDouble(o.toString());
    }

    public static String convertDateStr(String origin){
        return origin.substring(0,4)+"-"+origin.substring(4,6)+"-"+origin.substring(6,8);
    }

    public static String getCurrentTime(){
        //格式化
        return Constant.formatter3.format(LocalDateTime.now());
    }


    public static String encodeFileName(HttpServletRequest request, String fileName) {
        String name = "";

        String agent = request.getHeader("User-Agent");
        System.out.println(agent);
        try {
            if (agent.contains("Firefox")) {
                BASE64Encoder base64Encoder = new BASE64Encoder();
                name = "=?UTF-8?B?" + new String(base64Encoder.encode(fileName.getBytes("UTF-8"))) + "?=";
            } else {
                name = URLEncoder.encode(fileName, "UTF-8");
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        //System.out.println(name);
        return name;
    }

}
