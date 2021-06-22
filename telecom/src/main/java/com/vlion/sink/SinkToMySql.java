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

/**
 * @description:
 * @author: malichun
 * @time: 2021/6/21/0021 13:15
 */
public class SinkToMySql extends RichSinkFunction<Tuple2<String, Object>> {
    private Connection conn;
    private PreparedStatement psCustom;
    private PreparedStatement psOrderDetail;

    @Override
    public void open(Configuration parameters) throws Exception {
//        Class.forName(PropertiesUtils.getString("driver"));

        conn = DriverManager.getConnection(PropertiesUtils.getString("url"),
                PropertiesUtils.getString("username"), PropertiesUtils.getString("password")
        );

        psCustom = conn.prepareStatement("replace into consumer(order_id,template_id," +
                "cart_no,buyer_name,mobile_phone,receiver_prov,receiver_city,receiver_district,receiver_adress,`time`,`date`) " +
                "values(?,?,?,?,?,?,?,?,?,?,?)"); // 11个?

        psOrderDetail = conn.prepareStatement("replace into order_details(order_id," +
                "order_status,other_status,send_no,logistics_name,logistics_status,order_status_desc,is_last_invest,is_invest,etype" +
                ") values (?,?,?,?,?,?,?,?,?,?)"); // 10个?
    }

    @Override
    public void invoke(Tuple2<String, Object> value, Context context) throws Exception {
        String key = value.f0;
        Object e = value.f1;

        switch(key){
            case "316":
                Consumer consumer = (Consumer)e;
                psCustom.setString(1,consumer.getOrderId());
                psCustom.setString(2,consumer.getTemplateId());
                psCustom.setString(3,consumer.getCartNo());
                psCustom.setString(4,consumer.getBuyerName());
                psCustom.setString(5,consumer.getMobilePhone());
                psCustom.setString(6,consumer.getReceiverProv());
                psCustom.setString(7,consumer.getReceiverCity());
                psCustom.setString(8,consumer.getReceiverDistrict());
                psCustom.setString(9,consumer.getReceiverAddress());
                psCustom.setInt(10,Integer.parseInt(consumer.getTime().toString()));
                psCustom.setTimestamp(11,consumer.getDate());

                psCustom.execute();
                break;
            case "317":
                OrderDetail orderDetail = (OrderDetail)e;
                psOrderDetail.setString(1,orderDetail.getOrderId());
                psOrderDetail.setString(2,orderDetail.getOrderStatus());
                psOrderDetail.setString(3,orderDetail.getOtherStatus());
                psOrderDetail.setString(4,orderDetail.getSendNo());
                psOrderDetail.setString(5,orderDetail.getLogisticsName());
                psOrderDetail.setString(6,orderDetail.getLogisticsStatus());
                psOrderDetail.setString(7,orderDetail.getOrderStatusDesc());
                psOrderDetail.setString(8,orderDetail.getIsLastInvest());
                psOrderDetail.setString(9,orderDetail.getIsInvest());
                psOrderDetail.setString(10,orderDetail.getEtype());

                psOrderDetail.execute();
                break;
            default:

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
        if(psOrderDetail != null)
            psOrderDetail.close();
    }
}
