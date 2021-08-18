package com.vlion.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @description:
 * @author: malichun
 * @time: 2021/6/28/0028 10:08
 * 314日志
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class IntendUser {
    private String templateId; // 入口模版
    private String code ; // 状态码
    private String msg;// 错误原因
    private String time; // 时间（年月日小时），示例：2021062511
    private String carrier; // 运营商
//    private String pv ;// 频次
}
