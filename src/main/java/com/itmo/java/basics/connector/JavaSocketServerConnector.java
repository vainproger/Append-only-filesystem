package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.DatabaseInitializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.SegmentInitializer;
import com.itmo.java.basics.initialization.impl.TableInitializer;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.client.command.CreateDatabaseKvsCommand;
import com.itmo.java.client.command.KvsCommand;
import com.itmo.java.client.connection.ConnectionConfig;
import com.itmo.java.client.connection.SocketKvsConnection;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {

    private final DatabaseServer databaseServer;
    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();

    private final ServerSocket serverSocket;
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        this.databaseServer = databaseServer;
        this.serverSocket = new ServerSocket(config.getPort());
    }
 
     /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            try {
                Socket clientSocket = serverSocket.accept();
                clientIOWorkers.submit(() -> {
                    ClientTask clientTask = new ClientTask(clientSocket, databaseServer);
                    clientTask.run();
                });
            } catch (IOException e) {
                throw new UncheckedIOException("There's error while getting new client socket", e);
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        System.out.println("Stopping socket connector");
        try {
            clientIOWorkers.shutdownNow();
            connectionAcceptorExecutor.shutdownNow();
            serverSocket.close();
        } catch (IOException e) {
            throw new UncheckedIOException("There's error while closing socket", e);
        }
    }


    public static void main(String[] args) throws Exception {
        /*IntegrationTest test = new IntegrationTest();
        test.N1_startServer();
        test.N2_createClient();
        test.N3_createDatabase();
        test.N4_createTables();
        test.N5_initializeBeforeRandomIO();
        test.N6_randomIO();
        test.N7_close();*/
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {

        private final Socket clientSocket;
        private final DatabaseServer server;
        private final RespReader reader;
        private final RespWriter writer;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.clientSocket = client;
            this.server = server;
            try {
                reader = new RespReader(client.getInputStream());
                writer = new RespWriter(client.getOutputStream());
            } catch (IOException e) {
                throw new UncheckedIOException("There's error with getting I/O Streams", e);
            }
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
            try(CommandReader commandReader = new CommandReader(reader, server.getEnv())) {
                while (commandReader.hasNextCommand()) {
                    DatabaseCommand command = commandReader.readCommand();
                    DatabaseCommandResult databaseCommandResult = server.executeNextCommand(command).get();
                    writer.write(databaseCommandResult.serialize());
                }
            } catch (Exception e) {
                throw new UncheckedIOException("exception in run", (IOException) e);
            }
            close();
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                clientSocket.close();
                reader.close();
                writer.close();
            } catch (IOException e) {
                throw new UncheckedIOException("There's error while closing sockets", e);
            }
        }
    }
}
