package com.jinwang.mydb.utils;

import java.nio.ByteBuffer;

/**
 * @Author jinwang
 * @Date 2023/12/1 16:24
 * @Version 1.0 （版本号）
 */
public class Parser {
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

}
