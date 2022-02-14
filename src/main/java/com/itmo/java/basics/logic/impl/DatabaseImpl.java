package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.KvsIndex;
import com.itmo.java.basics.index.impl.MapBasedKvsIndex;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private Path databasePath;
    private String dataBaseName;
    private File db;
    private Map<String, Table> tablesList;


    public static Database initializeFromContext(DatabaseInitializationContext context) {

        DatabaseImpl newBase = new DatabaseImpl();
        newBase.dataBaseName = context.getDbName();
        newBase.databasePath = context.getDatabasePath();
        newBase.tablesList = context.getTables();
        return newBase;
    }


    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        DatabaseImpl newBD = new DatabaseImpl();
        newBD.tablesList = new HashMap<>();
        newBD.dataBaseName = dbName;
        if (dbName == null) {
            throw new DatabaseException("Null name!");
        }
        newBD.db = new File(String.valueOf(databaseRoot.resolve(dbName)));
        if (newBD.db.exists()) {
            throw new DatabaseException("File with with the " + dbName + " already exists!");
        }
        newBD.db.mkdir();
        return newBD;

    }

    @Override
    public String getName() {
        return dataBaseName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("Null name!");
        }
        if (tablesList.isEmpty()) {
            Table newTab = TableImpl.create(tableName, Paths.get(db.getAbsolutePath()), new TableIndex());
            tablesList.put(tableName, newTab);
        }
        else {
            if (tablesList.containsKey(tableName)) {
                throw new DatabaseException("Table with the" + tableName + "is already exists!");
            }
            else {
                Table newTab = TableImpl.create(tableName, Paths.get(db.getAbsolutePath()), new TableIndex());
                tablesList.put(tableName, newTab);
            }
        }
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (tableName == null | objectKey == null){
            throw new DatabaseException("Null name!");
        }
        if (tablesList.containsKey(tableName)) {
            tablesList.get(tableName).write(objectKey, objectValue);
        }
        else {
            throw new DatabaseException("No table with the " + tableName);
        }
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (objectKey == null | tableName == null){
            return Optional.empty();
        }
        if (!tablesList.containsKey(tableName)) {
            throw new DatabaseException("Can't find" + tableName + "in map!");
        }
        return tablesList.get(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (!tablesList.containsKey(tableName)) {
            throw new DatabaseException("Can't find" + tableName + "in map!");
        }
        tablesList.get(tableName).delete(objectKey);
    }
}
