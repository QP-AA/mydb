package com.jinwang.mydb.utils;

/**
 * @Author jinwang
 * @Date 2023/12/1 15:52
 * @Version 1.0 （版本号）
 */
public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
