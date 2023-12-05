package com.jinwang.mydb.backend.common;

import com.jinwang.mydb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 引用计数缓存策略
 * @Author jinwang
 * @Date 2023/12/4 21:06
 * @Version 1.0 （版本号）
 */
public abstract class AbstractCache<T> {

    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在获取某资源的线程

    private int maxResource;                           // 缓存中最大资源数
    private int cnt;                                   // 缓存中已有资源数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        // TODO: 是否可以先判断cache中是否有资源， 如果没有直接去数据源中获取而不是等待？
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                // 如果正在被获取，则需要等待一段时间之后重新请求获取该资源
                lock.unlock();
                // TODO: 尝试使用lock.Condition
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;  // 直接进入下一次循环
            }

            // 轮到该进程使用资源, 首先检查该资源是否在缓存中
            if (cache.containsKey(key)) {
                // 缓存中包含该资源
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);  // 更新资源引用次数
                lock.unlock();
                return obj;
            }

            // 缓存中没用该资源
            // 首先检查是否达到最大缓存个数
            if (maxResource > 0 && cnt == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;  // 缓存已满
            }
            cnt ++;
            getting.put(key, true);
            lock.unlock();
            break;
        }
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            // 如果我们无法从数据源中获取数据
            lock.lock();
            cnt --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);  // 由于是第一次从从资源中将数据放入缓存，所以值为1
        lock.unlock();

        return obj;
    }

    /**
     * 释放资源
     * @param key
     */
    protected void release(long key) {
        lock.lock();
        try {
            Integer ref = references.get(key) - 1;  // 获取引用次数
            if (ref == 0) {
                // 所有占用都已释放
                T obj = cache.get(key);  // 获取缓存中的数据
                releaseForCache(obj);    // 写回数据源
                references.remove(key);  // 移除引用
                cache.remove(key);
                cnt --;
            } else {
                // 还有占用
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存， 释放所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            keys.forEach(key -> {
                T obj = cache.get(key);
                releaseForCache(obj);
                cache.remove(key);
                references.remove(key);
            });
        } finally {
            lock.unlock();
        }
    }

    /**
     * 资源不在缓存
     * @param key
     * @return
     * @throws Exception
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 将资源驱逐回数据源
     * @param obj
     * @return
     */
    protected abstract void releaseForCache(T obj);
}
