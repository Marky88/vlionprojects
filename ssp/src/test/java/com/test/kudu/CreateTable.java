package com.test.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import java.util.LinkedList;
import java.util.List;

public class CreateTable {

    private static ColumnSchema newColumn(String name, Type  type, boolean iskey) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(iskey);
        return column.build();
    }

    public static void main(String[] args) throws KuduException {
// master 地址
        final String masteraddr = "www.bigdata03.com";
// 创建 kudu 的数据库链接
        KuduClient client = new KuduClient.KuduClientBuilder(masteraddr).defaultSocketReadTimeoutMs(6000).build();
// 设置表的 schema
        List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
        columns.add(newColumn("CompanyId", Type.INT32, true));
        columns.add(newColumn("WorkId", Type.INT32, false));
        columns.add(newColumn("Name", Type.STRING, false));
        columns.add(newColumn("Gender", Type.STRING, false));
        columns.add(newColumn("Photo", Type.STRING, false));
        Schema schema = new Schema(columns);
//创建表时提供的所有选项
        CreateTableOptions options = new CreateTableOptions();
// 设置表的 replica 备份和分区规则
        List<String> parcols = new LinkedList<String>();
        parcols.add("CompanyId");
//设置表的备份数
        options.setNumReplicas(1);
//设置 range 分区
        options.setRangePartitionColumns(parcols);
//设置 hash 分区和数量
        options.addHashPartitions(parcols, 3);
        try {
            client.createTable("person", schema, options);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        client.close();
    }
}
