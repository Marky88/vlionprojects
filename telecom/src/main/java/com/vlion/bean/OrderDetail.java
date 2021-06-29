package com.vlion.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class OrderDetail implements Serializable {
    private String orderId; // 订单id
    private String orderStatus; // 订单状态
    private String otherStatus; // 激活状态
    private String activeTime; // 激活时间
    private String sendNo; // 物流单号
    private String logisticsName; // 物流公司
    private String logisticsStatus; // 物流状态
    private String orderStatusDesc; // 订单状态/做废原因
    private String isLastInvest; // 激活后充值
    private String lastInvestTime; // 激活后充值时间
    private String isInvest; // 激活前充值
    private String investTime; // 激活前充值时间
    private String etype ; // 头条转化类型
}

