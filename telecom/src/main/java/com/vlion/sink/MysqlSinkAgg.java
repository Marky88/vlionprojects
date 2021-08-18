package com.vlion.sink;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.vlion.utils.PropertiesUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @description:
 * @author: malichun
 * @time: 2021/6/28/0028 17:16
 */
public class MysqlSinkAgg extends RichSinkFunction<Tuple2<Tuple5<String,String, String, String,String>, Long>>  {
//    private Connection conn;
//    private PreparedStatement psIntendUser;

    private DataSource dataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        //构建连接池
        Properties properties = new Properties();
        properties.setProperty("driverClassName", PropertiesUtils.getString("driver"));
        properties.setProperty("url", PropertiesUtils.getString("url"));
        properties.setProperty("username", PropertiesUtils.getString("username"));
        properties.setProperty("password", PropertiesUtils.getString("password"));
        properties.setProperty("maxActive", "2");
//        properties.setProperty("timeBetweenEvictionRunsMillis",);
//        properties.setProperty("minEvictableIdleTimeMillis",);
        try {
            dataSource = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void invoke(Tuple2<Tuple5<String,String, String, String, String>, Long> value, Context context) throws Exception {
//        System.out.println("sink invoke :"+ value);
//      arr[4], // 入口模版  arr[2], // 状态码
//                arr[3], // 错误原因
//                arr[1] // 时间戳
        Connection conn =null;
        PreparedStatement psIntendUser = null;
        try{
            conn = dataSource.getConnection();
            psIntendUser = conn.prepareStatement("replace into intend_user(template_id,code,msg,pv,`time`,`hour`,`carrier`) values(?,?,?,?,?,?,?)");
            //时间戳从context里面拿
//            long currentWaterMark = context.currentWatermark();
//            System.out.println("currentWaterMark"+currentWaterMark);
//            String format = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(currentWaterMark-1L));
            String template = value.f0.f0;
            String code = value.f0.f1;
            String msg = value.f0.f2;
            String carrier = value.f0.f3;
            String dayHour = value.f0.f4;
            String[] arr = dayHour.split(" ");
            long pv = value.f1;

            psIntendUser.setString(1,template);
            psIntendUser.setString(2,code);
            psIntendUser.setString(3,msg);
            psIntendUser.setLong(4,pv);
            psIntendUser.setString(5,arr[0]);
            psIntendUser.setString(6,arr[1]);
            psIntendUser.setString(7,carrier);
            psIntendUser.execute();

        }catch (Exception e){
            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime()) + " 报错:↓ " + value);
            e.printStackTrace();
        }finally {
            if(psIntendUser !=null){
                psIntendUser.close();
                psIntendUser=null;
            }
            if(conn != null){
                conn.close();;
                conn =null;
            }
        }

    }

    @Override
    public void close() throws Exception {
        //关闭连接和释放资源
        if (dataSource != null) {
            dataSource = null;
        }
    }

}
