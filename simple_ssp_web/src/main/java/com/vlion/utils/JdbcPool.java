package com.vlion.utils;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/22/0022 15:21
 */
public class JdbcPool implements DataSource {

    private static LinkedList<Connection> list = new LinkedList<Connection>();
    static{
        try{
            InputStream in = JdbcPool.class.getClassLoader().getResourceAsStream("db.properties");
            Properties prop = new Properties();
            prop.load(in);//加载配置文件

            String driver = prop.getProperty("driver");
            String url = prop.getProperty("url");
            String username = prop.getProperty("username");
            String password = prop.getProperty("password");

            Class.forName(driver); //加载驱动
            //连接池中创建10个Connection
            for(int i = 0; i < 10; i++){
                Connection conn = DriverManager.getConnection(url, username, password);
                System.out.println("获取到了连接" + conn);
                list.add(conn);
            }
        } catch(Exception e){
            e.printStackTrace();
            throw new ExceptionInInitializerError(e);
        }
    }
    @Override
    public synchronized Connection getConnection() throws SQLException {
        if(list.size() > 0){
            final Connection conn =  list.removeFirst();//删掉并返回给conn
            //          return conn;//这里不能直接return，因为用户使用完了后，调用conn.close()会操作数据库，并没有把这个conn返回给连接池中
            System.out.println("池大小是" + list.size());

            //下面用动态代理技术来写：
            //反射部分
            //动态代理技术：使用的是拦截技术
            return (Connection) Proxy.newProxyInstance(JdbcPool.class.getClassLoader(), new Class[]{Connection.class}, new InvocationHandler() {
                //这里的第二个参数原来为conn.getClass.getInterface()，不过会出错，最终改成了new Class[]{Connection.class}
                //原因见帖子：http://blog.csdn.net/njchenyi/article/details/3091092
                @Override
                public Object invoke(Object proxy, Method method, Object[] args)
                        throws Throwable {

                    if(!method.getName().equals("close")){//如果判断不是调用close方法，不管就是了
                        return method.invoke(conn, args);
                    }
                    else{//如果是调用close方法，将conn放到连接池里
                        list.add(conn);
                        System.out.println(conn + "被还到池中");
                        System.out.println("池大小为" + list.size());
                        return null;
                    }
                }
            });

        }
        else{
            throw new RuntimeException("对不起，数据库忙");
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {

    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
