package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.FileFilter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseServerInitializer implements Initializer {

    private DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {

        this.databaseInitializer = databaseInitializer;
    }
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        File check = new File(context.executionEnvironment().getWorkingPath().toString());
        if (!check.exists()) {
            check.mkdirs();
        }
        File[] arrFiles = check.listFiles();
        for (File i : arrFiles) {
            DatabaseInitializationContextImpl newContext = new DatabaseInitializationContextImpl(i.getName(), check.toPath());
            InitializationContextImpl notContext = new InitializationContextImpl(
                    context.executionEnvironment(),
                    newContext,
                    null,
                    null
            );
            databaseInitializer.perform(notContext);
        }
    }
}
