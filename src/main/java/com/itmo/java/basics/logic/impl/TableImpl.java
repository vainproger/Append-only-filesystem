package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.io.*;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.Vector;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {
    private String tableName;
    private File tableFolder;
    private TableIndex tabIndex;
    private Segment currentSegment;

    private TableImpl(String tablename, Path pathToDatabaseRoot, TableIndex tableIndex) {
        currentSegment = null;
        tableName = tablename;
        tableFolder = new File(String.valueOf(pathToDatabaseRoot.resolve(tableName)));
        tabIndex = tableIndex;
    }
    private TableImpl(String tablename, Path pathToDatabaseRoot, TableIndex tableIndex, Segment currentSegment) {
        this.currentSegment = currentSegment;
        tableName = tablename;
        tableFolder = new File(String.valueOf(pathToDatabaseRoot.resolve(tableName)));
        tabIndex = tableIndex;
    }
    public static Table create(String tablename, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        TableImpl newTable = new TableImpl(tablename, pathToDatabaseRoot, tableIndex);
        if (!newTable.tableFolder.exists()) {
            if (!newTable.tableFolder.mkdirs()) {
                throw new DatabaseException("Table with the" + tablename + "already exists!");
            }
        }
        else
            throw new DatabaseException("This" + tablename + "is already used");
        return new CachingTable(newTable);
    }

    public static Table initializeFromContext(TableInitializationContext context) {

        TableImpl newTable = new TableImpl(context.getTableName(), context.getTablePath(), context.getTableIndex(), context.getCurrentSegment());
        return new CachingTable(newTable);
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (currentSegment == null || currentSegment.isReadOnly()) {
            currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), tableFolder.toPath());
        }
        try {
            currentSegment.write(objectKey, objectValue);
            tabIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException e){
            throw new DatabaseException("Error with writing!", e);
        }

    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (tabIndex.searchForKey(objectKey).isEmpty()) {
            return Optional.empty();
        }
        try {
            return tabIndex.searchForKey(objectKey).get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("Error with reading!", e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if (objectKey == null) {
            throw new DatabaseException("Can't delete because of key is null! ");
        }
        if (currentSegment == null || currentSegment.isReadOnly()) {
            currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), tableFolder.toPath());
        }
        try {
            currentSegment.delete(objectKey);
            tabIndex.onIndexedEntityUpdated(objectKey, currentSegment);
        } catch (IOException e){
            throw new DatabaseException("Error with deleting object!", e);
        }
    }
}
