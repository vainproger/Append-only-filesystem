package com.itmo.java.basics.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {

    Properties properties = new Properties();

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream("server.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        try {
            if (this.getClass().getClassLoader().getResourceAsStream(name) != null) {
                properties.load(this.getClass().getClassLoader().getResourceAsStream(name));
            }else{
                FileInputStream fileInputStream = new FileInputStream(name);
                properties.load(fileInputStream);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        String workingPath = properties.getProperty("kvs.workingPath");
        String host = properties.getProperty("kvs.host");
        String stringPort = properties.getProperty("kvs.port");
        if (host == null){
            host = ServerConfig.DEFAULT_HOST;
        }
        int port;
        if (stringPort == null) {
            port = ServerConfig.DEFAULT_PORT;
        } else {
            port = Integer.parseInt(stringPort);
        }
        if (workingPath == null){
            workingPath = DatabaseConfig.DEFAULT_WORKING_PATH;
        }
        ServerConfig serverConfig = new ServerConfig(host, port);
        DatabaseConfig databaseConfig = new DatabaseConfig(workingPath);
        return new DatabaseServerConfig(serverConfig, databaseConfig);
    }
}
