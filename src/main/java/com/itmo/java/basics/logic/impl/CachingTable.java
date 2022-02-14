package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

public class CachingTable implements Table {
    private Table newTable;
    private DatabaseCache myDataBaseCache;

    private final int MAX_CACHE_CAPACITY = 10000;

    public CachingTable(Table myTable) {
        this.newTable = myTable;
        this.myDataBaseCache = new DatabaseCacheImpl(MAX_CACHE_CAPACITY);
    }

    @Override
    public String getName() {
        return newTable.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        newTable.write(objectKey, objectValue);
        myDataBaseCache.set(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        byte[] cacheValue = myDataBaseCache.get(objectKey);
        if (cacheValue != null) {
            return Optional.of(cacheValue);
        }
        else {
            return newTable.read(objectKey);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        newTable.delete(objectKey);
        myDataBaseCache.delete(objectKey);
    }
}
