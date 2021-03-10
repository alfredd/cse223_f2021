package edu.uci.ics.cse223;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.sql.SQLException;



public class CoordinatorServer {

    public static void main(String[] args) throws IOException, SQLException, InterruptedException {
        ConfigurationManager configurationManager = new ConfigurationManager();
        Integer serverPortNumber = configurationManager.getHostPort(0);
        String dbUrl = configurationManager.getDBUrl(0);
        DB db = new DB(0);
        Server server = null;
        try {
            server = ServerBuilder.forPort(serverPortNumber)
                    .addService(new CoordinatorService(db))
                    .addService(new TransactionService(db))
                    .build().start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Server started.");
        Server finalServer = server;
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (finalServer != null) {
                    finalServer.shutdown();
                }
                db.close();
            }
        });
        if (server!=null) {
            server.awaitTermination();
        }

    }
}
