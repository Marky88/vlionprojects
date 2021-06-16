package com.test.redis;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @description:
 * @author: malichun
 * @time: 2021/1/14/0014 15:27
 */
public class Test1 {
    public static void main(String[] args) {
        List<TestConfig> bodyParam = new ArrayList<TestConfig>();

        bodyParam.stream().map(e -> {
            return e.method("a");
        }).collect(Collectors.toList());


        for(TestConfig testConfig:bodyParam){
            testConfig.sayHello();
        }
    }
}

class TestConfig {
    private int a;
    private int b;

    public int getB() {
        return b;
    }

    public void setB(int b) {
        this.b = b;
    }

    @Test
    public void name() {
    }

    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }

    public void sayHello() {
        System.out.println("sayHello~~~");
    }

    public String method(String s){
        return s+"~~~";
    }
}
