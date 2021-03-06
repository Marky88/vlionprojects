package com.test.kafka.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author malichun
 * @Title:
 * @Package
 * @Description:
 * @date 2020/7/17 001717:03
 */
public class InterceptorProducer {
    public static void main(String[] args) {
        // 1 设置配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers","www.bigdata04.com:9092,www.bigdata05.com:9092,www.bigdata06.com:9092");
        props.put("acks","all");
        props.put("retries",1);
        props.put("batch.size",16384);
        props.put("linger.ms",1);
        props.put("buffer.memeory",33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2.构建拦截器
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.test.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.test.kafka.interceptor.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        String topic = "first";
        Producer<String, String> producer = new KafkaProducer<>(props);

        /*
        Producer<String,String> producer = new KafkaProducer<>(props);
        for(int i=0;i<100;i++){
            producer.send(new ProducerRecord<String, String>("first",Integer.toString(i),Integer.toString(i)));
        }
        */
        // 3 发送消息
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message" + i);
            producer.send(record);
        }

        // 4 一定要关闭producer，这样才会调用interceptor的close方法
        producer.close();


    }


}
