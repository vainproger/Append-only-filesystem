package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespObject;

/**
 * Результат успешной команды
 */
public class SuccessDatabaseCommandResult implements DatabaseCommandResult {

    private byte[] message;

    public SuccessDatabaseCommandResult(byte[] payload) {
        if (payload == null) {
            this.message = null;
        }
        else {
            this.message = payload;
        }
    }

    @Override
    public String getPayLoad() {

        if (message != null) {
            return new String(message);
        }
        return null;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    /**
     * Сериализуется в {@link RespBulkString}
     */
    @Override
    public RespObject serialize() {
        return new RespBulkString(message);
    }
}
