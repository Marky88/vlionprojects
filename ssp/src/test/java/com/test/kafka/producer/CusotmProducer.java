package com.test.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author malichun
 * @Title:
 * @Package
 * @Description:
 * @date 2020/7/17 001715:09
 */
public class CusotmProducer {
    //不带回调函数
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","www.bigdata04.com:9092,www.bigdata05.com:9092,www.bigdata06.com:9092");
        props.put("acks","all");
        props.put("retries",1);
        props.put("batch.size",16384);
        props.put("linger.ms",1);
        props.put("buffer.memeory",33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String,String> producer = new KafkaProducer<>(props);
        for(int i=0;i<100;i++){
            producer.send(new ProducerRecord<String, String>("first",Integer.toString(i),Integer.toString(i)));
        }

        producer.close();


    }
}
