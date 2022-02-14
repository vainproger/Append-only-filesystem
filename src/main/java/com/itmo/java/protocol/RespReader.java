package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class RespReader implements AutoCloseable {

    private final InputStream inputStream;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    private void readCRLF() throws IOException {
        readCheck(CR);
        readCheck(LF);
    }

    private void readCheck(byte check) throws IOException{
        byte[] symbol;
        try{
            symbol = inputStream.readNBytes(1);
            if (symbol.length == 0){
                throw new EOFException("Symbol is null " + new String(symbol));
            }
            if (symbol[0] != check){
                throw new IOException("Unexpected symbols " + new String(symbol));
            }
        } catch (IOException e){
            throw new IOException("Read IOException", e);
        }
    }

    public RespReader(InputStream is) {
        this.inputStream = is;
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        if (inputStream.readNBytes(1)[0] != RespArray.CODE){
            return false;
        }
        return true;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        try {
            String nexSymbol = new String(inputStream.readNBytes(1));
            switch (nexSymbol) {
                case "-":
                    return readError();
                case "$":
                    return readBulkString();
                case "*":
                    return readArray();
                case "!":
                    return readCommandId();
                default:
                    throw new IOException("Unexpected value: " + nexSymbol);
            }
        } catch (IOException e){
            throw new IOException("Error while reading symbols", e);
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        try {
            StringBuilder stringBuilder = new StringBuilder();
            byte[] symbol = new byte[1];
            while ((symbol = inputStream.readNBytes(1))[0] != LF) {
                stringBuilder.append(new String(symbol));
            }
            return new RespError(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e){
            throw new IOException("Error while reading symbols", e);
        }
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        try {
            byte[] size;
            StringBuilder builder = new StringBuilder();
            while ((size = inputStream.readNBytes(1))[0] != CR) {
                builder.append(new String(size));
            }
            if (Integer.parseInt(builder.toString()) == RespBulkString.NULL_STRING_SIZE) {
                inputStream.read(new byte[]{LF});
                return RespBulkString.NULL_STRING;
            }
            inputStream.read(new byte[]{LF});
            byte[] neededString = inputStream.readNBytes(Integer.parseInt(builder.toString()));
            readCRLF();
            return new RespBulkString(neededString);
        } catch (IOException e){
            throw new IOException("Error while reading symbols", e);
        }
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        try {
            byte[] size;
            StringBuilder builder = new StringBuilder();
            while ((size = inputStream.readNBytes(1))[0] != CR) {
                builder.append(new String(size));
            }
            int s = Integer.parseInt(builder.toString());
            RespObject[] respArray = new RespObject[s];
            inputStream.read(new byte[]{LF});
            for (int i = 0; i < s; i++) {
                respArray[i] = readObject();
            }
            return new RespArray(respArray);
        } catch (IOException e){
            throw new IOException("Error while reading symbols", e);
        }
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        try {
            byte[] commandIdBytes = inputStream.readNBytes(4);
            int commandId = (commandIdBytes[0] << 24)
            + (commandIdBytes[1] << 16)
            + (commandIdBytes[2] << 8)
            + (commandIdBytes[3] << 0);
            readCRLF();
            return new RespCommandId(commandId);
        }catch(IOException e){
            throw new IOException("Reading Id exception", e);
        }
    }


    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
