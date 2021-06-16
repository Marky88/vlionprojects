package com.test.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author malichun
 * @Title:
 * @Package
 * @Description:
 * @date 2020/7/17 001717:01
 */
public class CounterInterceptor implements ProducerInterceptor<String,String> {
    private int errorCounter = 0 ;
    private int successCounter = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // 统计成功和失败的次数
        if (exception == null) {
            successCounter++;
        } else {
            errorCounter++;
        }
    }

    @Override
    public void close() {
        //保存结果
        System.out.println("Successful sent : "+successCounter);
        System.out.println("Failed sent : "+ errorCounter);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
