package com.test.redis;

import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.Set;

public class TestRedis {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("172.16.189.196",6379);
        System.out.println(jedis.ping());

        jedis.select(1);
        Set<String> keys = jedis.keys("*");
        for(Iterator iterator = keys.iterator();iterator.hasNext();){
            String key = (String) iterator.next();
            System.out.println(key);
        }
        System.out.println(jedis.dbSize());
    }
}
