package com.lilinzhe.hadoop.mr.day05.json;

import com.alibaba.fastjson.JSON;

public class JsonTest01 {
    public static void main(String[] args) {
        User user = new User("留言",18);
        String json = JSON.toJSONString(user);
        System.out.println(json);

    }
}
class User{
    private String name;
    private int age;

    public User(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}