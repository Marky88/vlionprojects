package com.test.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author malichun
 * @Title:
 * @Package
 * @Description:  带回调函数API
 * @date 2020/7/17 001715:17
 */
public class CustomProducer1 {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "www.bigdata04.com:9092,www.bigdata05.com:9092,www.bigdata06.com:9092");//kafka集群，broker-list
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        // 自定义分区
        props.put("partitioner.class", "com.test.kafka.producer.CustomPartitioner");

        Producer<String,String> producer = new KafkaProducer<String, String>(props);
        for(int i =0;i<100;i++){
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)+"\t----vvvv"), new Callback() {
                //回调函数,该方法会在Producer收到ack时调用,为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        System.out.println("success->"+metadata.offset());
                    }else{
                        exception.printStackTrace();
                    }
                }
            });
        }


        producer.close();
    }

}
