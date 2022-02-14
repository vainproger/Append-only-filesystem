package com.itmo.java.client.client;

import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespObject;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {

    private final String databaseName;
    private final KvsConnection connection;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.databaseName = databaseName;
        this.connection = connectionSupplier.get();
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {

        RespObject result;
        try {
            KvsCommand createDBCommand = new CreateDatabaseKvsCommand(databaseName);
            result = connection.send(createDBCommand.getCommandId(), createDBCommand.serialize());
            if (result.isError()) {
                throw new DatabaseExecutionException("There is a error in connection method!");
            }
            return result.asString();
        } catch(ConnectionException e){
            throw new DatabaseExecutionException("There is an error in connection!", e);
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {

        RespObject result;
        try {
            KvsCommand createTableCommand = new CreateTableKvsCommand(databaseName, tableName);
            result = connection.send(createTableCommand.getCommandId(), createTableCommand.serialize());
            if (result.isError()) {
                throw new DatabaseExecutionException("There is a error in connection method!");
            }
            return result.asString();
        } catch(ConnectionException e){
            throw new DatabaseExecutionException("There is an error in connection!", e);
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {

        RespObject result;
        try {
            KvsCommand getCommand = new GetKvsCommand(databaseName, tableName, key);
            result = connection.send(getCommand.getCommandId(), getCommand.serialize());
            if (result.isError()) {
                throw new DatabaseExecutionException("There is a error in connection method!");
            }
            return result.asString();
        } catch(ConnectionException e){
            throw new DatabaseExecutionException("There is an error in connection!", e);
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {

        RespObject result;
        try {
            KvsCommand setCommand = new SetKvsCommand(databaseName, tableName, key, value);
            result = connection.send(setCommand.getCommandId(), setCommand.serialize());
            if (result.isError()) {
                throw new DatabaseExecutionException("There is a error in connection method!");
            }
            return result.asString();
        } catch(ConnectionException e){
            throw new DatabaseExecutionException("There is an error in connection!", e);
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {

        RespObject result;
        try {
            KvsCommand deleteCommand = new DeleteKvsCommand(databaseName, tableName, key);
            result = connection.send(deleteCommand.getCommandId(), deleteCommand.serialize());
            if (result.isError()) {
                throw new DatabaseExecutionException("There is a error in connection method!");
            }
            return result.asString();
        } catch(ConnectionException e){
            throw new DatabaseExecutionException("There is an error in connection!", e);
        }
    }
}
