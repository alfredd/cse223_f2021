package edu.uci.ics.cse223;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.sql.SQLException;

public class CohortServer {

    public static void main(String[] args) throws IOException, SQLException, InterruptedException {
//        System.out.println(args.length);
        if (args.length!=1) {
            System.out.println("Input cohort host ID (1, 2, 3).");
        }
        Integer cohortID = Integer.decode(args[0]);
        if (cohortID<=0) {
            System.out.println("Error in use of Cohort host ID. Use a number greater than 0");
            System.exit(1);
        }
        ConfigurationManager configurationManager = new ConfigurationManager();
        Integer serverPortNumber = configurationManager.getHostPort(cohortID);
        DB db = new DB(cohortID);

        Server server = null;
        try {
            server = ServerBuilder.forPort(serverPortNumber)
                    .addService(new CohortService(cohortID, db))
                    .build().start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Server finalServer = server;
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (finalServer !=null) {
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
