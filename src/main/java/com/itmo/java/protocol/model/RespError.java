package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static com.itmo.java.protocol.model.RespBulkString.NULL_STRING_SIZE;

/**
 * Сообщение об ошибке в RESP протоколе
 */
public class RespError implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '-';

    private final byte[] message;

    public RespError(byte[] message) {
        if (message == null){
            this.message = null;
        }
        else {
            this.message = message;
        }
    }

    /**
     * Ошибка ли это? Ответ - да
     *
     * @return true
     */
    @Override
    public boolean isError() {
        return true;
    }

    @Override
    public String asString() {
        if (this.message == null){
            return null;
        }
        return new String(message);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(message);
        os.write(CRLF);
    }
}
