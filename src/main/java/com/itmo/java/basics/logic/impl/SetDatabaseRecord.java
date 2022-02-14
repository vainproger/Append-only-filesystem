package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

import java.nio.charset.StandardCharsets;

/**
 * Запись в БД, означающая добавление значения по ключу
 */
public class SetDatabaseRecord implements WritableDatabaseRecord {

    private final String keyObj;
    private final int sizeOfRecord;
    private final  Integer keyObjSize;
    private final byte[] valueObj;
    private final Integer valueObjSize;

    public SetDatabaseRecord(String keyObject, byte[] valueObject){
        keyObj = keyObject;
        keyObjSize = keyObject.length();
        valueObj = valueObject;
        valueObjSize = valueObj.length;
        sizeOfRecord = keyObj.length() + valueObj.length + 8;
    }

    @Override
    public byte[] getKey() {

        return keyObj.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getValue() {

        return valueObj;
    }

    @Override
    public long size() {

        return sizeOfRecord;
    }

    @Override
    public boolean isValuePresented() {

        return true;
    }

    @Override
    public int getKeySize() {

        return keyObjSize;
    }

    @Override
    public int getValueSize() {

        return valueObjSize;
    }
}