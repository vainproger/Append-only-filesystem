package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {

    private final RespReader reader;
    private final RespWriter writer;
    private final Socket socket;

    public SocketKvsConnection(ConnectionConfig config)  {
        try {
            this.socket = new Socket(config.getHost(), config.getPort());
            reader = new RespReader(this.socket.getInputStream());
            writer = new RespWriter(this.socket.getOutputStream());
        } catch (IOException exception) {
            throw new UncheckedIOException("Error while connecting to server", exception);
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {

        try {
            writer.write(command);
            return reader.readObject();
        } catch(IOException e) {
            throw new ConnectionException(e.getMessage(), e);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() throws IOException {
        try {
            socket.close();
            reader.close();
            writer.close();
        } catch (IOException e) {
            throw new IOException("Error while closing socket", e);
        }
    }
}
