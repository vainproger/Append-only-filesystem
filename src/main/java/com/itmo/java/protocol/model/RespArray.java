package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '*';

    private final List<RespObject> objects;

    public RespArray(RespObject... objects) {
        this.objects = new ArrayList<>();
        this.objects.addAll(Arrays.asList(objects));
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        StringJoiner result = new StringJoiner(" ");
        for (RespObject i : objects){
            result.add(i.asString());
        }
        return result.toString();
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(Integer.toString(objects.size()).getBytes(StandardCharsets.UTF_8));
        os.write(CRLF);
        for (RespObject i : objects){
            i.write(os);
        }
    }
    public List<RespObject> getObjects() {
        return objects;
    }
}
