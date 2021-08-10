package com.vlion.bean;

import java.io.Serializable;
import java.sql.Timestamp;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Consumer implements Serializable {
    // 316日志
    private String orderId; //订单id
    private String templateId;//入口模版
    private String cartNo; //入网身份证
    private String buyerName; // 入网姓名
    private String mobilePhone; // 联系电话
    private String receiverProv ;// 收货人所在省
    private String receiverCity; // 收货人所在市
    private String receiverDistrict; // 收货人所在区
    private String receiverAddress;// 收货人详细地址
    private String planId; // 广告计划id
    private String creativeId;// 广告创意id
    private String comboType; // 套餐类型
    private String isChooseNum; // 是否选号
    private Long time; // 时间戳
    private Timestamp date;//日期
    private String orderMobilePhone;// 下单号码
    private String channelId; //渠道id
    private String sourceType; // 来源方式打标说明
    private String flowType; // 引流平台打标说明
    private String pid; //一级代理
    private String eid; //二级代理
    private String ip; // 用户ip
    private String carrier; // 运营商
}
