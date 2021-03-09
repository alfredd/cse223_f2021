package edu.uci.ics.cse223;

import io.grpc.stub.StreamObserver;

public class TransactionService extends TransactionServiceGrpc.TransactionServiceImplBase {

    private final DB db;

    public TransactionService(DB db) {
        this.db=db;
    }

    @Override
    public void executeTransaction(Twopc.SQL request, StreamObserver<Twopc.TransactionStatus> responseObserver) {
        super.executeTransaction(request, responseObserver);
    }
}
