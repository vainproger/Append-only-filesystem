package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

public class TableInitializer implements Initializer {

    SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File check = new File(context.currentTableContext().getTablePath().toString());
        if (!check.exists()) {
            throw new DatabaseException("File " + check.getName() + "does not exist!");
        }
        String[] arrFiles = check.list();
        Arrays.sort(arrFiles);
        for (String i : arrFiles) {
            SegmentInitializationContextImpl newContext = new SegmentInitializationContextImpl(i,
                    context.currentTableContext().getTablePath(), 0);
                segmentInitializer.perform(new InitializationContextImpl(
                        context.executionEnvironment(),
                        context.currentDbContext(),
                        context.currentTableContext(),
                        newContext));
        }
        context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
    }
}
