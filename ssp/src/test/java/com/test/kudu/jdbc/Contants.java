package com.test.kudu.jdbc;


import java.sql.*;

/**
 * @author malichun
 * @Title:
 * @Package
 * @Description:
 * @date 2020/7/15 001516:11
 */
public class Contants {
    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    private static String CONNECTION_URL = "jdbc:impala://www.bigdata01.com:21050/default;auth=noSasl";
    //定义数据库连接
    static Connection conn = null;
    //定义PrepareStatement对象
    static PreparedStatement ps = null;
    //定义查询的结果集
    static ResultSet rs = null;

    //数据库连接
    public static Connection getConn() {
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(CONNECTION_URL);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return conn;
    }

    //创建一个表
    public static void createTable() {
        conn = getConn();
        String sql = "CREATE TABLE impala_kudu_test" +
                "(" +
                "companyId BIGINT," +
                "workId BIGINT," +
                "name STRING," +
                "gender STRING," +
                "photo STRING," +
                "PRIMARY KEY(companyId)" +
                ")" +
                "PARTITION BY HASH PARTITIONS 16 " +
                "STORED AS KUDU " +
                "TBLPROPERTIES (" +
                "'kudu.table_name' = 'impala_kudu_test'" +
                ");";

        try {
            ps = conn.prepareStatement(sql);
            ps.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    //查询数据
    public static ResultSet queryRows() {
        try {
            String sql = "select * from impala_kudu_test";
            ps = getConn().prepareStatement(sql);
            rs = ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    //打印结果
    public static void printRows(ResultSet rs) {
        try {
            while (rs.next()) {
                //获取表的每一行字段信息
                int companyId = rs.getInt("companyId");
                int workId = rs.getInt("workId");
                String name = rs.getString("name");
                String gender = rs.getString("gender");
                String photo = rs.getString("photo");
                System.out.print("companyId:" + companyId + " ");
                System.out.print("workId:" + workId + " ");
                System.out.print("name:" + name + " ");
                System.out.print("gender:" + gender + " ");
                System.out.println("photo:" + photo);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    //插入数据
    public static void insertRows(Person person) {
        conn = getConn();
        String sql = "insert  into  table impala_kudu_test(companyId,workId,name,gender,photo) values(?,?,?,?,?)";
        try {
            ps = conn.prepareStatement(sql);
            //给占位符？赋值
            ps.setInt(1, person.getCompanyId());
            ps.setInt(2, person.getWorkId());
            ps.setString(3, person.getName());
            ps.setString(4, person.getGender());
            ps.setString(5, person.getPhoto());
            ps.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (ps != null) {
                try {
//关闭
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
//关闭
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    //更新数据
//更新数据
    public static void updateRows(Person person) {
//定义执行的 sql 语句
        String sql = "update impala_kudu_test set workId=" + person.getWorkId() +
                ",name='" + person.getName() + "' ," + "gender='" + person.getGender() + "' ," +
                "photo='" + person.getPhoto() + "'  where companyId=" + person.getCompanyId();
        try {
            ps = getConn().prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                try {
//关闭
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
            if (conn != null) {
                try {
//关闭
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    //删除数据
    public static void deleteRows(int companyId) {
        //定义 sql 语句
        String sql = "delete from impala_kudu_test where companyId=" + companyId;
        try {
            ps = getConn().prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    //删除表
    public static void dropTable() {
        String sql="drop table if exists impala_kudu_test";
        try {
            ps =getConn().prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
