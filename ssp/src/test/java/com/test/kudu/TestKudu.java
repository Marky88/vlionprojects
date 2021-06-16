package com.test.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.Test;


import java.util.ArrayList;
import java.util.LinkedList;

/**
 * @author malichun
 * @Title:
 * @Package
 * @Description:
 * @date 2020/7/13 001317:07
 */
public class TestKudu {
    //声明全局变量 KuduClient 后期通过它来操作 kudu 表
    private KuduClient kuduClient;
    //指定 kuduMaster 地址
    private String kuduMaster;
    //指定表名
    private String tableName;


    /**
     * 初始化
     */
    @Before
    public void init() {
//初始化操作
        kuduMaster = "www.bigdata03.com:7051";
//指定表名
        tableName = "student";
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultSocketReadTimeoutMs(10000);
        kuduClient = kuduClientBuilder.build();
    }


    /**
     * 创建表
     *
     * @throws KuduException
     */
    @Test
    public void createTable() throws KuduException {
//判断表是否存在，不存在就构建
        if (!kuduClient.tableExists(tableName)) {
//构建创建表的 schema 信息-----就是表的字段和类型
            ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("age", Type.INT32).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("sex", Type.INT32).build());
            Schema schema = new Schema(columnSchemas);
//指定创建表的相关属性
            CreateTableOptions options = new CreateTableOptions();
            ArrayList<String> partitionList = new ArrayList<String>();
//指定 kudu 表的分区字段是什么
            partitionList.add("id"); // 按照 id.hashcode % 分区数 = 分区号
            options.addHashPartitions(partitionList, 6);
            kuduClient.createTable(tableName, schema, options);
        }
    }


    /**
     * 向表加载数据
     */
    @Test
    public void insertTable() throws KuduException {
//向表加载数据需要一个 kuduSession 对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
//需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);
        for (int i = 1; i <= 10; i++) {
            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("id", i);
            row.addString("name", "zhangsan-" + i);
            row.addInt("age", 20 + i);
            row.addInt("sex", i % 2);
            kuduSession.apply(insert);//最后实现执行数据的加载操作
        }
    }


    /**
     * 查询表的数据结果
     */
    @Test
    public void queryData() throws KuduException {
//构建一个查询的扫描器
        KuduScanner.KuduScannerBuilder kuduScannerBuilder =
                kuduClient.newScannerBuilder(kuduClient.openTable(tableName));
        ArrayList<String> columnsList = new ArrayList<String>();
        columnsList.add("id");
        columnsList.add("name");
        columnsList.add("age");
        columnsList.add("sex");
        kuduScannerBuilder.setProjectedColumnNames(columnsList);
//返回结果集
        KuduScanner kuduScanner = kuduScannerBuilder.build();
//遍历
        while (kuduScanner.hasMoreRows()) {
            RowResultIterator rowResults = kuduScanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult row = rowResults.next();
                int id = row.getInt("id");
                String name = row.getString("name");
                int age = row.getInt("age");
                int sex = row.getInt("sex");
                System.out.println("id=" + id + "  name=" + name + "  age=" + age + " sex=" + sex);
            }
        }
    }


    /**
     * 修改表的数据
     */
    @Test
    public void updateData() throws KuduException {
//修改表的数据需要一个 kuduSession 对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
//需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);
//Update update = kuduTable.newUpdate();
        Upsert upsert = kuduTable.newUpsert(); //如果 id 存在就表示修改，不存在就新增
        PartialRow row = upsert.getRow();
        row.addInt("id", 100);
        row.addString("name", "zhangsan-100");
        row.addInt("age", 100);
        row.addInt("sex", 0);
        kuduSession.apply(upsert);//最后实现执行数据的修改操作
    }


    /**
     * 删除数据
     */
    @Test
    public void deleteData() throws KuduException {
//删除表的数据需要一个 kuduSession 对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
//需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);
        Delete delete = kuduTable.newDelete();
        PartialRow row = delete.getRow();
        row.addInt("id", 100);
        kuduSession.apply(delete);//最后实现执行数据的删除操作
    }


    @Test
    public void dropTable() throws KuduException {
        if (kuduClient.tableExists(tableName)) {
            kuduClient.deleteTable(tableName);
        }
    }


    private static ColumnSchema newColumn(String name, Type  type, boolean iskey) {
        ColumnSchema.ColumnSchemaBuilder column = new  ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(iskey);
        return column.build();
    }

    /**
     * 测试分区：
     * RangePartition
     */
    @Test
    public void testRangePartition() throws KuduException {
//设置表的 schema
        LinkedList<ColumnSchema> columnSchemas = new
                LinkedList<ColumnSchema>();
        columnSchemas.add(newColumn("CompanyId", Type.INT32, true));
        columnSchemas.add(newColumn("WorkId", Type.INT32, false));
        columnSchemas.add(newColumn("Name", Type.STRING, false));
        columnSchemas.add(newColumn("Gender", Type.STRING, false));
        columnSchemas.add(newColumn("Photo", Type.STRING, false));
        //创建 schema
        Schema schema = new Schema(columnSchemas);
        //创建表时提供的所有选项
        CreateTableOptions tableOptions = new CreateTableOptions();
//设置副本数
        tableOptions.setNumReplicas(1);
        //设置范围分区的规则
        LinkedList<String> parcols = new LinkedList<String>();
        parcols.add("CompanyId");
//设置按照那个字段进行 range 分区
        tableOptions.setRangePartitionColumns(parcols);
        /**
         * range
         * 0 < value < 10
         * 10 <= value < 20
         * 20 <= value < 30
         * ........
         * 80 <= value < 90
         * */
        int count = 0;
        for (int i = 0; i < 10; i++) {
//范围开始
            PartialRow lower = schema.newPartialRow();
            lower.addInt("CompanyId", count);
//范围结束
            PartialRow upper = schema.newPartialRow();
            count += 10;
            upper.addInt("CompanyId", count);
//设置每一个分区的范围
            tableOptions.addRangePartition(lower, upper);
        }
        try {
            kuduClient.createTable("student", schema, tableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        kuduClient.close();
    }


    /**
     * 测试分区：
     * hash 分区
     */
    @Test
    public void testHashPartition() throws KuduException {
//设置表的 schema
        LinkedList<ColumnSchema> columnSchemas = new LinkedList<ColumnSchema>();
        columnSchemas.add(newColumn("CompanyId", Type.INT32, true));
        columnSchemas.add(newColumn("WorkId", Type.INT32, false));
        columnSchemas.add(newColumn("Name", Type.STRING, false));
        columnSchemas.add(newColumn("Gender", Type.STRING, false));
        columnSchemas.add(newColumn("Photo", Type.STRING, false));
//创建 schema
        Schema schema = new Schema(columnSchemas);
//创建表时提供的所有选项
        CreateTableOptions tableOptions = new CreateTableOptions();
//设置副本数
        tableOptions.setNumReplicas(1);
//设置范围分区的规则
        LinkedList<String> parcols = new LinkedList<String>();
        parcols.add("CompanyId");
//设置按照那个字段进行 range 分区
        tableOptions.addHashPartitions(parcols, 6);
        try {
            kuduClient.createTable("dog", schema, tableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        kuduClient.close();
    }

/**
 * 测试分区：
 * 多级分区
 * Multilevel Partition
 * 混合使用 hash 分区和 range 分区
 *
 * 哈希分区有利于提高写入数据的吞吐量，而范围分区可以避免
 tablet 无限增长问题，
 * hash 分区和 range 分区结合，可以极大的提升 kudu 的性能
 */
    @Test
    public void testMultilevelPartition() throws KuduException
    {
//设置表的 schema
        LinkedList<ColumnSchema> columnSchemas = new LinkedList<ColumnSchema>();
        columnSchemas.add(newColumn("CompanyId", Type.INT32,true));
        columnSchemas.add(newColumn("WorkId", Type.INT32,false));
        columnSchemas.add(newColumn("Name", Type.STRING,false));
        columnSchemas.add(newColumn("Gender", Type.STRING,false));
        columnSchemas.add(newColumn("Photo", Type.STRING,false));
//创建 schema
        Schema schema = new Schema(columnSchemas);
//创建表时提供的所有选项
        CreateTableOptions tableOptions = new CreateTableOptions();
//设置副本数
        tableOptions.setNumReplicas(1);
//设置范围分区的规则
        LinkedList<String> parcols = new LinkedList<String>();
        parcols.add("CompanyId");
//hash 分区
        tableOptions.addHashPartitions(parcols,5);
//range 分区
        int count=0;
        for(int i=0;i<10;i++){
            PartialRow lower = schema.newPartialRow();
            lower.addInt("CompanyId",count);
            count+=10;
            PartialRow upper = schema.newPartialRow();
            upper.addInt("CompanyId",count);
            tableOptions.addRangePartition(lower,upper);
        }
        try {
            kuduClient.createTable("cat",schema,tableOptions);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        kuduClient.close();
    }







}

