package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Vector;


public class SegmentInitializer implements Initializer {
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        SegmentIndex segmentIndex = new SegmentIndex();
        ArrayList<String> keys = new ArrayList<>();
        long currentOffset = 0;
        try (FileInputStream segmentInputStream = new FileInputStream(String.valueOf(context.currentSegmentContext().getSegmentPath()));) {
            DatabaseInputStream dataInputStream = new DatabaseInputStream(segmentInputStream);
            Optional<DatabaseRecord> neededRecord = dataInputStream.readDbUnit();
            while (neededRecord.isPresent()) {
                segmentIndex.onIndexedEntityUpdated(new String(neededRecord.get().getKey()),
                        new SegmentOffsetInfoImpl(currentOffset));
                currentOffset += neededRecord.get().size();
                keys.add(new String(neededRecord.get().getKey()));
                neededRecord = dataInputStream.readDbUnit();
            }
            SegmentInitializationContextImpl currentSegmentCont = new SegmentInitializationContextImpl(
                    context.currentSegmentContext().getSegmentName(), context.currentSegmentContext().getSegmentPath(),
                    currentOffset, segmentIndex);
            Segment segment = SegmentImpl.initializeFromContext(currentSegmentCont);
            for (String i : keys) {
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(i,
                        segment);
            }
            context.currentTableContext().updateCurrentSegment(segment);
        } catch (FileNotFoundException e) {
            throw new DatabaseException("File " + context.currentSegmentContext().getSegmentName() + " not found", e);
        } catch (IOException e) {
            throw new DatabaseException("IOexception in file " + context.currentSegmentContext().getSegmentName(), e);
        }
    }
}
