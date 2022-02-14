package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {

    private String currentWorkingPath;

    private Map<String, Database> databaseList;

    public ExecutionEnvironmentImpl() {
        databaseList = new HashMap<>();
        currentWorkingPath = "db_files";
    }


    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        databaseList = new HashMap<>();
        currentWorkingPath = config.getWorkingPath();
    }

    @Override
    public Optional<Database> getDatabase(String name) {

        if (!databaseList.containsKey(name)){
            return Optional.empty();
        }

        return Optional.of(databaseList.get(name));
    }

    @Override
    public void addDatabase(Database db) {

        databaseList.put(db.getName(), db);

    }

    @Override
    public Path getWorkingPath() {
        return Path.of(currentWorkingPath);
    }
}
