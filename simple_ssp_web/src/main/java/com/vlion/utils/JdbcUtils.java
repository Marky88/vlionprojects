package com.vlion.utils;

import java.sql.*;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/22/0022 15:32
 */
public class JdbcUtils {


//    private static JdbcPool pool = new JdbcPool();//定义一个连接池
    public static Connection getConnection() throws SQLException {
        return JdbcPool2.getConnection();//直接从连接池中获取一个Connection
    }

    public static void release(Connection conn, Statement st, ResultSet rs) {
        if(rs != null){
            try{
                rs.close();
            }catch(Exception e) {
                e.printStackTrace();
            }
            rs = null;
        }
        if(st != null){
            try{
                st.close();
            }catch(Exception e) {
                e.printStackTrace();
            }
            st = null;
        }

        if(conn != null){
            try{
                conn.close();  //释放连接
            }catch(Exception e) {
                e.printStackTrace();
            }
            conn = null;
        }
    }

    public static void queryBatch(String sql, Object[] params, Process process){
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
             conn = getConnection();
             pstmt = conn.prepareStatement(sql);
             if(params!=null) {
                 for (int i = 0; i < params.length; i++) {
                     pstmt.setObject(i+1, params[i]);
                 }
             }
             resultSet = pstmt.executeQuery();
            process.queryCallBack(resultSet);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            release(conn, pstmt, resultSet);
        }

    }

}

