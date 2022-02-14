package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.impl.DatabaseImpl;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class DatabaseInitializer implements Initializer {

    private TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        File check = new File(initialContext.currentDbContext().getDatabasePath().toString());
        if (!(check.isDirectory() || check.exists())) {
            throw new DatabaseException("File " + check.getName() + " already exist or it's a directory");
        }
        File[] arrFiles = check.listFiles();
        if (arrFiles != null) {
            Arrays.sort(arrFiles, Comparator.comparingLong(File::lastModified));
            for (File i : arrFiles) {
                TableInitializationContextImpl newContext =
                        new TableInitializationContextImpl(i.getName(),
                                initialContext.currentDbContext().getDatabasePath(), new TableIndex());
                InitializationContextImpl notContext = new InitializationContextImpl(
                        initialContext.executionEnvironment(),
                        initialContext.currentDbContext(),
                        newContext,
                        null);
                tableInitializer.perform(notContext);
            }
            initialContext.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(initialContext.currentDbContext()));
        }
    }
}
