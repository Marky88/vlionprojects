package com.vlion.sink;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.vlion.bean.Consumer;
import com.vlion.bean.OrderDetail;
import com.vlion.utils.PropertiesUtils;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * @time: 2021/6/21/0021 13:15
 */
public class MysqlSinkOneByOne extends RichSinkFunction<Tuple2<String, Object>> {
    //    private Connection conn;
//    private PreparedStatement psCustom;
//    private PreparedStatement psOrderDetail;
//    private Statement statement;
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
    public void invoke(Tuple2<String, Object> value, Context context) throws Exception {
        String key = value.f0;
        Object e = value.f1;
        Connection conn = dataSource.getConnection();
        Statement statement = conn.createStatement();

        switch (key) {
            case "316":
                Consumer consumer = (Consumer) e;
                StringBuilder updateSql = new StringBuilder("update consumer set ");
                if (checkNotNull(consumer.getTemplateId())) {
                    updateSql.append(" template_id='").append(consumer.getTemplateId()).append("',");
                }
                if (checkNotNull(consumer.getCartNo())) {
                    updateSql.append(" cart_no='").append(consumer.getCartNo()).append("',");
                }
                if (checkNotNull(consumer.getBuyerName())) {
                    updateSql.append(" buyer_name='").append(consumer.getBuyerName()).append("',");
                }
                if (checkNotNull(consumer.getMobilePhone())) {
                    updateSql.append(" mobile_phone='").append(consumer.getMobilePhone()).append("',");
                }
                if (checkNotNull(consumer.getReceiverProv())) {
                    updateSql.append(" receiver_prov='").append(consumer.getReceiverProv()).append("',");
                }
                if (checkNotNull(consumer.getReceiverCity())) {
                    updateSql.append(" receiver_city='").append(consumer.getReceiverCity()).append("',");
                }
                if (checkNotNull(consumer.getReceiverDistrict())) {
                    updateSql.append(" receiver_district='").append(consumer.getReceiverDistrict()).append("',");
                }
                if (checkNotNull(consumer.getReceiverAddress())) {
                    updateSql.append(" receiver_adress='").append(consumer.getReceiverAddress()).append("',");
                }
                if (checkNotNull(consumer.getPlanId())) {
                    updateSql.append(" plan_id='").append(consumer.getPlanId()).append("',");
                }
                if (checkNotNull(consumer.getCreativeId())) {
                    updateSql.append(" creative_id='").append(consumer.getCreativeId()).append("',");
                }
                if (checkNotNull(consumer.getComboType())) {
                    updateSql.append(" combo_type='").append(consumer.getComboType()).append("',");
                }
                if (checkNotNull(consumer.getIsChooseNum())) {
                    updateSql.append(" is_choose_num='").append(consumer.getIsChooseNum()).append("',");
                }
                if (!(consumer.getTime() == null || consumer.getTime() == 0L)) {
                    updateSql.append(" time=").append(consumer.getTime()).append(",");
                }
                if (!(consumer.getDate() == null)) {
                    updateSql.append(" `date`='").append(consumer.getDate()).append("',");
                }
                if(checkNotNull(consumer.getSourceType())){
                    updateSql.append(" source_type='").append(consumer.getSourceType()).append("',");
                }
                if(checkNotNull(consumer.getFlowType())){
                    updateSql.append(" flow_type='").append(consumer.getFlowType()).append("',");
                }
                if(checkNotNull(consumer.getPid())){
                    updateSql.append(" pid='").append(consumer.getPid()).append("',");
                }
                if(checkNotNull(consumer.getEid())){
                    updateSql.append(" eid='").append(consumer.getEid()).append("',");
                }

                updateSql.append(" where order_id='").append(consumer.getOrderId()).append("'");
                String sql = updateSql.toString().replaceAll(", +where", " where");
//                    System.out.println("sql: " + sql);
                PreparedStatement psCustom = null;
                try {
                    boolean execute = statement.execute(sql);

                    if (statement.getUpdateCount() == 0) { // 如果没有更新
                        psCustom = conn.prepareStatement("insert into consumer(order_id,template_id," +
                                "cart_no,buyer_name,mobile_phone,receiver_prov,receiver_city," +
                            "receiver_district,receiver_adress,plan_id,creative_id,combo_type,is_choose_num,`time`,`date`, source_type,flow_type,pid,eid) " +
                                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                        //插入
                        psCustom.setString(1, consumer.getOrderId());
                        psCustom.setString(2, consumer.getTemplateId());
                        psCustom.setString(3, consumer.getCartNo());
                        psCustom.setString(4, consumer.getBuyerName());
                        psCustom.setString(5, consumer.getMobilePhone());
                        psCustom.setString(6, consumer.getReceiverProv());
                        psCustom.setString(7, consumer.getReceiverCity());
                        psCustom.setString(8, consumer.getReceiverDistrict());
                        psCustom.setString(9, consumer.getReceiverAddress());
                        psCustom.setString(10, consumer.getPlanId());
                        psCustom.setString(11, consumer.getCreativeId());
                        psCustom.setString(12, consumer.getComboType());
                        psCustom.setString(13, consumer.getIsChooseNum());
                        psCustom.setInt(14, Integer.parseInt(consumer.getTime().toString()));
                        psCustom.setString(15, consumer.getDate().toString());
                        psCustom.setString(16,consumer.getSourceType());
                        psCustom.setString(17,consumer.getFlowType());
                        psCustom.setString(18,consumer.getPid());
                        psCustom.setString(19,consumer.getEid());

                        psCustom.execute();
                    }
                } catch (Exception e2) {
                    System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime()) + " 报错:↓ " + consumer);
                    e2.printStackTrace();
                } finally {
                    if (statement != null) {
                        statement.close();
                        statement = null;
                    }
                    if (psCustom != null) {
                        psCustom.close();
                        psCustom = null;
                    }

                    if (conn != null) {
                        conn.close();
                        conn = null;
                    }
                }

                break;
            case "317":
                OrderDetail orderDetail = (OrderDetail) e;
                StringBuilder updateSql1 = new StringBuilder("update order_details set ");
                if (checkNotNull(orderDetail.getOrderStatus())) {
                    updateSql1.append(" order_status='").append(orderDetail.getOrderStatus()).append("',");
                }
                if (checkNotNull(orderDetail.getOtherStatus())) {
                    updateSql1.append(" other_status='").append(orderDetail.getOtherStatus()).append("',");
                }
                if (checkNotNull(orderDetail.getActiveTime())) {
                    updateSql1.append(" active_time='").append(orderDetail.getActiveTime()).append("',");
                }
                if (checkNotNull(orderDetail.getSendNo())) {
                    updateSql1.append(" send_no='").append(orderDetail.getSendNo()).append("',");
                }
                if (checkNotNull(orderDetail.getLogisticsName())) {
                    updateSql1.append(" logistics_name='").append(orderDetail.getLogisticsName()).append("',");
                }
                if (checkNotNull(orderDetail.getLogisticsStatus())) {
                    updateSql1.append(" logistics_status='").append(orderDetail.getLogisticsStatus()).append("',");
                }
                if (checkNotNull(orderDetail.getOrderStatusDesc())) {
                    updateSql1.append(" order_status_desc='").append(orderDetail.getOrderStatusDesc()).append("',");
                }
                if (checkNotNull(orderDetail.getIsLastInvest())) {
                    updateSql1.append(" is_last_invest='").append(orderDetail.getIsLastInvest()).append("',");
                }
                if (checkNotNull(orderDetail.getLastInvestTime())) {
                    updateSql1.append(" last_invest_time='").append(orderDetail.getLastInvestTime()).append("',");
                }
                if (checkNotNull(orderDetail.getIsInvest())) {
                    updateSql1.append(" is_invest='").append(orderDetail.getIsInvest()).append("',");
                }
                if (checkNotNull(orderDetail.getEtype())) {
                    updateSql1.append(" etype='").append(orderDetail.getEtype()).append("',");
                }
                updateSql1.append(" where order_id='").append(orderDetail.getOrderId()).append("'");
                String sql1 = updateSql1.toString().replaceAll(", +where", " where");
//                    System.out.println("sql1: " + sql1);

                PreparedStatement psOrderDetail = null;
                try {
                    statement.execute(sql1);
                    if (statement.getUpdateCount() == 0) { // 如果没有更新
                        //插入
                        psOrderDetail = conn.prepareStatement("insert into order_details(order_id," +
                                "order_status,other_status,active_time,send_no,logistics_name,logistics_status,order_status_desc,is_last_invest," +
                                "last_invest_time,is_invest,invest_time,etype" +
                                ") values (?,?,?,?,?,?,?,?,?,?,?,?,?)"); // 13个?

                        psOrderDetail.setString(1, orderDetail.getOrderId());
                        psOrderDetail.setString(2, orderDetail.getOrderStatus());
                        psOrderDetail.setString(3, orderDetail.getOtherStatus());
                        psOrderDetail.setString(4, orderDetail.getActiveTime());
                        psOrderDetail.setString(5, orderDetail.getSendNo());
                        psOrderDetail.setString(6, orderDetail.getLogisticsName());
                        psOrderDetail.setString(7, orderDetail.getLogisticsStatus());
                        psOrderDetail.setString(8, orderDetail.getOrderStatusDesc());
                        psOrderDetail.setString(9, orderDetail.getIsLastInvest());
                        psOrderDetail.setString(10, orderDetail.getLastInvestTime());
                        psOrderDetail.setString(11, orderDetail.getIsInvest());
                        psOrderDetail.setString(12, orderDetail.getInvestTime());
                        psOrderDetail.setString(13, orderDetail.getEtype());

                        psOrderDetail.execute();
                    }
                } catch (Exception e3) {
                    System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime()) + " 报错:↓ " + orderDetail);
                    e3.printStackTrace();
                } finally {
                    if (statement != null) {
                        statement.close();
                        statement = null;
                    }
                    if (psOrderDetail != null) {
                        psOrderDetail.close();
                        psOrderDetail = null;
                    }

                    if (conn != null) {
                        conn.close();
                        conn = null;
                    }
                }
                break;
            default:
        }


    }


    @Override
    public void close() throws Exception {
        //关闭连接和释放资源
        if (dataSource != null) {
            dataSource = null;
        }
    }

    private boolean checkNotNull(String col) {
        return !(col == null || col.trim().equals(""));
    }
}
