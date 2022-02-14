package com.itmo.java.basics;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private ExecutionEnvironment environment;
    private DatabaseServerInitializer initializer;

    /**
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */

    private DatabaseServer(ExecutionEnvironment env, DatabaseServerInitializer initializer){
        this.environment = env;
        this.initializer = initializer;
    }

    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {

        InitializationContext context = new InitializationContextImpl(env,
                null,
                null,
                null);

        initializer.perform(context);

        return new DatabaseServer(env, initializer);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        return CompletableFuture.supplyAsync(() -> {
            List<RespObject> objects = message.getObjects();
            String commandName = objects.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString();
            DatabaseCommand dbCommand = DatabaseCommands.valueOf(commandName).getCommand(environment, objects);
            return dbCommand.execute();
        }, executorService);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, executorService);
    }
    public ExecutionEnvironment getEnv() {
        return environment;
    }
}