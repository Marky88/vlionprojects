package com.vlion.utils;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/26/0026 18:15
 */
public class JdbcPool2 {
    //初始化连接池
    private static final DataSource dataSource = init();


    public static  DataSource init()  {

        InputStream in = JdbcPool.class.getClassLoader().getResourceAsStream("db.properties");
        Properties prop = new Properties();
        try {
            prop.load(in);//加载配置文件
        } catch (IOException e) {
            e.printStackTrace();
        }

        String driver = prop.getProperty("driver");
        String url = prop.getProperty("url");
        String username = prop.getProperty("username");
        String password = prop.getProperty("password");

        Properties properties = new Properties();
//        properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
//        properties.setProperty("driverClassName", "com.mysql.cj.jdbc.Driver")

        properties.setProperty("driverClassName",driver);
        properties.setProperty("url", url);
        properties.setProperty("username", username);
        properties.setProperty("password", password);
        properties.setProperty("maxActive", prop.getProperty("jdbc.datasource.size"));
        DataSource dataSource = null;
        try {
            dataSource = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataSource;
    }

    //获取mysql连接
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
