package com.test.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import scala.actors.threadpool.Arrays;

import java.util.Properties;

/**
 * @author malichun
 * @Title:
 * @Package
 * @Description:  自定义消费者, 手动维护offset
 * @date 2020/7/17 001715:52
 */
public class CustomCustomer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "www.bigdata04.com:9092,www.bigdata05.com:9092,www.bigdata06.com:9092");
        props.put("group.id", "test");//消费者组，只要group.id相同，就属于同一个消费者组
        props.put("enable.auto.commit", "false");//自动提交offset

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        consumer.subscribe(Arrays.asList(new String[]{"first"}));

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String,String> record:records){
                System.out.printf("offset = %d, key = %s ,value=%s, partition = %s %n",record.offset(),record.key(),record.value(),record.partition());
            }
            consumer.commitSync();
        }



    }


}
