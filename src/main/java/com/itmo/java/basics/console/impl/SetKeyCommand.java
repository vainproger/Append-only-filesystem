package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment environment;
    private final String databaseName;
    private final String tableName;
    private final String key;
    private final String value;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.environment = env;
        if (commandArgs.size() != 6) {
            throw new IllegalArgumentException("Wrong number of arguments in commandArgs!");
        }
        this.databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        this.tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        this.key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
        this.value = commandArgs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString();
    }

    /**
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        byte[] result;
        try {
            if (this.environment.getDatabase(databaseName).isEmpty()) {
                return DatabaseCommandResult.error("No database with" + databaseName + "!");
            }
            Optional<byte[]> optionalResult = environment.getDatabase(databaseName).get().read(tableName, key);
            environment.getDatabase(databaseName).get().write(tableName, key, value.getBytes(StandardCharsets.UTF_8));
            if (optionalResult.isPresent()){
                result = optionalResult.get();
                return DatabaseCommandResult.success(result);
            }else{
                return DatabaseCommandResult.success(null);
            }
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }
    }
}
