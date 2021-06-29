package com.vlion.sink;

import com.vlion.bean.IntendUser;
import com.vlion.utils.PropertiesUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * @description:
 * @author: malichun
 * @time: 2021/6/28/0028 17:16
 */
public class MysqlSinkAgg extends RichSinkFunction<Tuple2<Tuple4<String, String, String,String>, Long>>  {
    private Connection conn;
    private PreparedStatement psIntendUser;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection(PropertiesUtils.getString("url"),
                PropertiesUtils.getString("username"), PropertiesUtils.getString("password")
        );
        //这个表，（每小时+每个状态码+h5模板）统计一条数据入库
        psIntendUser = conn.prepareStatement("replace into intend_user(template_id,code,msg,pv,`time`) values(?,?,?,?,?)"); // 5个

    }

    @Override
    public void invoke(Tuple2<Tuple4<String, String, String, String>, Long> value, Context context) throws Exception {
//        System.out.println("sink invoke :"+ value);
//      arr[4], // 入口模版  arr[2], // 状态码
//                arr[3], // 错误原因
//                arr[1] // 时间戳
        String template = value.f0.f0;
        String code = value.f0.f1;
        String msg = value.f0.f2;
        String dayHour = value.f0.f3;
        long pv = value.f1;

        psIntendUser.setString(1,template);
        psIntendUser.setString(2,code);
        psIntendUser.setString(3,msg);
        psIntendUser.setLong(4,pv);
        psIntendUser.setString(5,dayHour);
        psIntendUser.execute();

    }

    @Override
    public void close() throws Exception {
        if(psIntendUser!=null) psIntendUser.close();
        if(conn!=null) conn.close();
    }

}
