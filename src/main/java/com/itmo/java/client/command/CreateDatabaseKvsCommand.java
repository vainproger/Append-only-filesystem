package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;

/**
 * Команда для создания бд
 */
public class CreateDatabaseKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_DATABASE";

    private final int id = idGen.incrementAndGet();

    private final String databaseName;

    /**
     * Создает объект
     *
     * @param databaseName имя базы данных
     */
    public CreateDatabaseKvsCommand(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {

        return new RespArray(new RespCommandId(id),
                             new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8)),
                             new RespBulkString(databaseName.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public int getCommandId() {
        return idGen.get();
    }
}
