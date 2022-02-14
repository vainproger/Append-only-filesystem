package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {

    private final int capacity;
    private final Map<String, byte[]> newCache;

    public DatabaseCacheImpl(int maxCapacity) {
        this.capacity = maxCapacity;
        this.newCache = new LinkedHashMap<String, byte[]>(capacity, 1f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
                return size() > capacity;
            }
        };
    }

    @Override
    public byte[] get(String key) {

        return newCache.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        newCache.put(key, value);
    }

    @Override
    public void delete(String key) {
        newCache.remove(key);
    }
}
