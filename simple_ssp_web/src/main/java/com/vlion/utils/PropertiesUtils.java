package com.vlion.utils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/24/0024 10:15
 */
public class PropertiesUtils {
    private static Properties props ;
    static{
           InputStream in = JdbcPool.class.getClassLoader().getResourceAsStream("common.properties");
              props = new Properties();
            try {
                props.load(in);//加载配置文件
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    public static String getString(String key) {
        return props.getProperty(key);
    }
}
