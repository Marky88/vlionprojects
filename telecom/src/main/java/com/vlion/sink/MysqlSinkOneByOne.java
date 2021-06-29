package com.vlion.sink;

import com.vlion.bean.Consumer;
import com.vlion.bean.OrderDetail;
import com.vlion.utils.PropertiesUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * @description:
 * @author: malichun
 * @time: 2021/6/21/0021 13:15
 */
public class MysqlSinkOneByOne extends RichSinkFunction<Tuple2<String, Object>> {
    private Connection conn;
    private PreparedStatement psCustom;
    private PreparedStatement psOrderDetail;
    private Statement statement;

//    @Override
//    public void open(Configuration parameters) throws Exception {
////        Class.forName(PropertiesUtils.getString("driver"));
//
//        conn = DriverManager.getConnection(PropertiesUtils.getString("url"),
//                PropertiesUtils.getString("username"), PropertiesUtils.getString("password")
//        );
//
//        psCustom = conn.prepareStatement("replace into consumer(order_id,template_id," +
//                "cart_no,buyer_name,mobile_phone,receiver_prov,receiver_city,receiver_district,receiver_adress,`time`,`date`) " +
//                "values(?,?,?,?,?,?,?,?,?,?,?)"); // 11个?
//
//        psOrderDetail = conn.prepareStatement("replace into order_details(order_id," +
//                "order_status,other_status,send_no,logistics_name,logistics_status,order_status_desc,is_last_invest,is_invest,etype" +
//                ") values (?,?,?,?,?,?,?,?,?,?)"); // 10个?
//    }
//
//    @Override
//    public void invoke(Tuple2<String, Object> value, Context context) throws Exception {
//        String key = value.f0;
//        Object e = value.f1;
//
//        switch(key){
//            case "316":
//                Consumer consumer = (Consumer)e;
//                psCustom.setString(1,consumer.getOrderId());
//                psCustom.setString(2,consumer.getTemplateId());
//                psCustom.setString(3,consumer.getCartNo());
//                psCustom.setString(4,consumer.getBuyerName());
//                psCustom.setString(5,consumer.getMobilePhone());
//                psCustom.setString(6,consumer.getReceiverProv());
//                psCustom.setString(7,consumer.getReceiverCity());
//                psCustom.setString(8,consumer.getReceiverDistrict());
//                psCustom.setString(9,consumer.getReceiverAddress());
//                psCustom.setInt(10,Integer.parseInt(consumer.getTime().toString()));
//                psCustom.setTimestamp(11,consumer.getDate());
//
//                psCustom.execute();
//                break;
//            case "317":
//                OrderDetail orderDetail = (OrderDetail)e;
//                psOrderDetail.setString(1,orderDetail.getOrderId());
//                psOrderDetail.setString(2,orderDetail.getOrderStatus());
//                psOrderDetail.setString(3,orderDetail.getOtherStatus());
//                psOrderDetail.setString(4,orderDetail.getSendNo());
//                psOrderDetail.setString(5,orderDetail.getLogisticsName());
//                psOrderDetail.setString(6,orderDetail.getLogisticsStatus());
//                psOrderDetail.setString(7,orderDetail.getOrderStatusDesc());
//                psOrderDetail.setString(8,orderDetail.getIsLastInvest());
//                psOrderDetail.setString(9,orderDetail.getIsInvest());
//                psOrderDetail.setString(10,orderDetail.getEtype());
//
//                psOrderDetail.execute();
//                break;
//            default:
//
//        }
//
//
//    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        Class.forName(PropertiesUtils.getString("driver"));
        conn = DriverManager.getConnection(PropertiesUtils.getString("url"),
                PropertiesUtils.getString("username"), PropertiesUtils.getString("password")
        );
        statement = conn.createStatement();


        psCustom = conn.prepareStatement("insert into consumer(order_id,template_id," +
                "cart_no,buyer_name,mobile_phone,receiver_prov,receiver_city,receiver_district,receiver_adress,plan_id,creative_id,combo_type,is_choose_num,`time`,`date`) " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"); // 15个?

        psOrderDetail = conn.prepareStatement("insert into order_details(order_id," +
                "order_status,other_status,active_time,send_no,logistics_name,logistics_status,order_status_desc,is_last_invest," +
                "last_invest_time,is_invest,invest_time,etype" +
                ") values (?,?,?,?,?,?,?,?,?,?,?,?,?)"); // 13个?
    }

    @Override
    public void invoke(Tuple2<String, Object> value, Context context) throws Exception {
        String key = value.f0;
        Object e = value.f1;
        try {
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
                    if(checkNotNull(consumer.getComboType())){
                        updateSql.append(" combo_type='").append(consumer.getComboType()).append("',");
                    }
                    if(checkNotNull(consumer.getIsChooseNum())){
                        updateSql.append(" is_choose_num='").append(consumer.getIsChooseNum()).append("',");
                    }
                    if (!(consumer.getTime() == null || consumer.getTime() == 0L)) {
                        updateSql.append(" time=").append(consumer.getTime()).append(",");
                    }
                    if (!(consumer.getDate() == null)) {
                        updateSql.append(" `date`='").append(consumer.getDate()).append("'");
                    }

                    updateSql.append(" where order_id='").append(consumer.getOrderId()).append("'");
                    String sql = updateSql.toString().replaceAll(", +where", " where");
//                    System.out.println("sql: " + sql);
                    boolean execute = statement.execute(sql);
                    if (statement.getUpdateCount() == 0) { // 如果没有更新
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

                        psCustom.execute();
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
                    statement.execute(sql1);
                    if (statement.getUpdateCount() == 0) { // 如果没有更新
                        //插入
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
                    break;
                default:
            }
        } catch (Exception e1) {
            System.out.println("报错: "+ e);
            e1.printStackTrace();
        }


    }


    @Override
    public void close() throws Exception {
        //关闭连接和释放资源
        if (conn != null) {
            conn.close();
        }
        if (psCustom != null) {
            psCustom.close();
        }
        if (psOrderDetail != null)
            psOrderDetail.close();
        if (statement != null) statement.close();
    }

    private boolean checkNotNull(String col) {
        return !(col == null || col.trim().equals(""));
    }
}
