package com.vlion.bean;

import java.io.Serializable;
import java.sql.Timestamp;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Consumer implements Serializable {
    private String orderId; //订单id
    private String templateId;//入口模版
    private String cartNo; //入网身份证
    private String buyerName; // 入网姓名
    private String mobilePhone; // 联系电话
    private String receiverProv ;// 收货人所在省
    private String receiverCity; // 收货人所在市
    private String receiverDistrict; // 收货人所在区
    private String receiverAddress;// 收货人详细地址
    private Long time; // 时间戳
    private Timestamp date;//日期

}
