package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер, большие значения (>100000) записываются в последний сегмент, если он не read-only
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {
    private String segmentName;
    private Path tableRootPath;
    private static final int segmentSizeMax = 100000;
    private long segmentSizeCurrent;
    private File txtSegment;
    private SegmentIndex segIndex;

    private SegmentImpl(String segmentname, Path tablerootPath){
        segmentSizeCurrent = 0;
        segIndex = new SegmentIndex();
        segmentName = segmentname;
        tableRootPath = tablerootPath;
        txtSegment = new File(String.valueOf(tableRootPath.resolve(segmentName)));
    }

    private SegmentImpl(String segmentname, Path tablerootPath, SegmentIndex segmentIndex, long size){
        segmentSizeCurrent = size;
        segIndex = segmentIndex;
        segmentName = segmentname;
        tableRootPath = tablerootPath;
        txtSegment = new File(String.valueOf(tableRootPath));
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        if (segmentName == null) {
            throw new DatabaseException("Null segment name");
        }
        if (tableRootPath == null) {
            throw new DatabaseException("Path is null!");
        }
        SegmentImpl newSegment = new SegmentImpl(segmentName, tableRootPath);
        if (Files.exists(newSegment.txtSegment.toPath())) {
            throw new DatabaseException("File already exists!");
        }
        try {
            if (!newSegment.txtSegment.createNewFile())
                throw new DatabaseException("Dir error");
        } catch(IOException e) {
            throw new DatabaseException("Error with creating file!", e);
        }
        return newSegment;

    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        SegmentImpl newSegment = new SegmentImpl(context.getSegmentName(), context.getSegmentPath(), context.getIndex(), context.getCurrentSize());
        return newSegment;
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {

        if (isReadOnly()) {
            return false;
        }
        if (objectValue == null){
            return delete(objectKey);
        }
        SetDatabaseRecord newRecord = new SetDatabaseRecord(objectKey, objectValue);

        try (FileOutputStream fout = new FileOutputStream(txtSegment, true);
        DatabaseOutputStream newOut = new DatabaseOutputStream(fout)) {
            segIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(segmentSizeCurrent));
            segmentSizeCurrent += newOut.write(newRecord);
        }
        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {

        if (segIndex.searchForKey(objectKey).isEmpty()) {
            return Optional.empty();
        }
        try (FileInputStream fin = new FileInputStream(txtSegment);
        DatabaseInputStream newIn = new DatabaseInputStream(fin)) {

            if (segIndex.searchForKey(objectKey).get().getOffset() != newIn.skip(segIndex.searchForKey(objectKey).get().getOffset())) {
                throw new IOException("Error with skipping bytes");
            }
            Optional<DatabaseRecord> neededRecord = newIn.readDbUnit();
            if (!neededRecord.get().isValuePresented()){
                return Optional.empty();
            }
            if (neededRecord.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(neededRecord.get().getValue());
        }
    }

    @Override
    public boolean isReadOnly() {
        return segmentSizeCurrent >= segmentSizeMax;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        RemoveDatabaseRecord newRecord = new RemoveDatabaseRecord(objectKey);
        if (isReadOnly()) {
            return false;
        }
        try (FileOutputStream fout = new FileOutputStream(txtSegment, true);
        DatabaseOutputStream newOut = new DatabaseOutputStream(fout)) {
            segIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(segmentSizeCurrent));
            segmentSizeCurrent += newOut.write(newRecord);
        }
        return true;
    }
}
