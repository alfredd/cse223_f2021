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

    public void executeTransaction(Twopc.Transaction transaction) {
        Twopc.TransactionStatus response = blockingStub.executeTransaction(transaction);
        if (response.getStatus() == Twopc.Status.COMMITTED) {
            System.out.println("Transaction committed. ");
        } else {
            System.out.println("Transaction aborted.");
        }

    }

    public static void main(String[] args) {
        TransactionClientFileReader clientFileReader = new TransactionClientFileReader(args[0]);
        clientFileReader.parseFileAndExecute();
    }
}
