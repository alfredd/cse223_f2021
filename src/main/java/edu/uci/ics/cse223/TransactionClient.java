package edu.uci.ics.cse223;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

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

    public void executeTransaction(Twopc.SQL transactionSQL) {
        Twopc.TransactionStatus response = blockingStub.executeTransaction(transactionSQL);
        if (response.getStatus() == Twopc.Status.COMMITTED) {
            System.out.println("Transaction committed. ");
        } else {
            System.out.println("Transaction aborted.");
        }

    }

    public static void main(String[] args) throws IOException {


        /**
         * For every 10 txns
         * SQL.addstatement(insert)
         * add txn_i as txnid
         * client.executeTransaction(request).
         */
        TransactionClient client = new TransactionClient();
        Twopc.SQL transaction = Twopc.SQL.newBuilder().setId("t91")
                .addStatement(
                        "INSERT INTO thermometerobservation VALUES " +
                                "('19', 80, '2017-11-08 00:00:00', 't91')")
//                .addStatement("INSERT INTO thermometerobservation VALUES ('16', 8, '2017-11-08 05:38:00', 't8')")
                .build();
         client.executeTransaction(transaction);
    }
}
