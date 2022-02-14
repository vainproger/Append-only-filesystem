package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

/**
 * Запись в БД, означающая удаление значения по ключу
 */
public class RemoveDatabaseRecord implements WritableDatabaseRecord {

    private final String keyObj;
    private final Integer keyObjSize;

    public RemoveDatabaseRecord(String keyObject){
        keyObj = keyObject;
        keyObjSize = keyObj.length();
    }

    @Override
    public byte[] getKey() {

        return keyObj.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getValue() {

        return null;
    }

    @Override
    public long size() {

        return (8 + keyObj.length());
    }

    @Override
    public boolean isValuePresented() {
        return false;
    }

    @Override
    public int getKeySize() {
        return keyObjSize;
    }

    @Override
    public int getValueSize() {
        return -1;
    }
}