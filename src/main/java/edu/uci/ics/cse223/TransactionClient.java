package edu.uci.ics.cse223;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TransactionClient {
    private final ManagedChannel channel;
    private int port;
    private TransactionServiceGrpc.TransactionServiceBlockingStub blockingStub;

    public TransactionClient() throws IOException {
        ConfigurationManager cm = new ConfigurationManager();
        this.port = cm.getHostPort(0);
        this.channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        blockingStub = TransactionServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public boolean isConnected() {
        return !channel.isTerminated();
    }

    public void executeTransaction(Twopc.Transaction transaction) {
        Twopc.TransactionStatus response = blockingStub.executeTransaction(transaction);
        if (response.getStatus() == Twopc.Status.COMMITTED) {
            System.out.println("Transaction committed. ");
        } else {
            System.out.println("Transaction aborted.");
        }

    }

    public static void main(String[] args) {
        BufferedReader reader;
        String line = "";
        StringBuilder query = new StringBuilder("");
        try {
            TransactionClient client = new TransactionClient();
            reader = new BufferedReader(new FileReader(
                    new File(args[0]))
            );
            line = reader.readLine();
            int cnt = 0, txnId = 0;
            while (line != null) {
                if (line.toLowerCase().contains("insert")) {
                    query.append(line).append(";");
                    cnt++;
                }
                line = reader.readLine();
                if (cnt == 10) {
                    txnId++;
                    Twopc.SQL transaction = Twopc.SQL.newBuilder().setId("t" + String.valueOf(txnId))
                            .addStatement(query.toString())
                            .build();
                    client.executeTransaction(transaction);
                    cnt = 0;
                    query.delete(0, query.length());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


//        TransactionClient client = new TransactionClient();
//        Twopc.SQL transaction = Twopc.SQL.newBuilder().setId("t91")
//                .addStatement(
//                        "INSERT INTO thermometerobservation VALUES " +
//                                "('19', 80, '2017-11-08 00:00:00', 't91')")
////                .addStatement("INSERT INTO thermometerobservation VALUES ('16', 8, '2017-11-08 05:38:00', 't8')")
//                .build();
//         client.executeTransaction(transaction);
    }
}
