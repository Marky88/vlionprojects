package com.test.kudu.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * @author malichun
 * @Title:
 * @Package
 * @Description:
 * @date 2020/7/15 001516:31
 */
public class ImpalaJdbcClient {
    public static void main(String[] args) {
        Connection conn = Contants.getConn();
//创建一个表
//        Contants.createTable();
//插入数据
        Contants.insertRows(new Person(1,100,"lisi","male","lisi-photo"));
//查询表的数据
        ResultSet rs = Contants.queryRows();
        Contants.printRows(rs);
//更新数据
        Contants.updateRows(new Person(1,200,"zhangsan","male","zhangsan-photo"));
//删除数据
        Contants.deleteRows(1);
//删除表
        Contants.dropTable();
    }
}