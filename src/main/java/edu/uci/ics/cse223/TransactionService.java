package edu.uci.ics.cse223;

import io.grpc.stub.StreamObserver;

public class TransactionService extends TransactionServiceGrpc.TransactionServiceImplBase {

    private final DB db;

    public TransactionService(DB db) {
        this.db=db;
    }

    @Override
    public void executeTransaction(Twopc.SQL request, StreamObserver<Twopc.TransactionStatus> responseObserver) {
        /**
         * To handle individual transactions, create individual threads. ThreadLooper.
         * Log incoming transactions.
         * Call Cohorts with the SQL statement to prepare the transaction.
         * Based on Response from all Cohorts, Commit OR Abort.
         * Finally call responseObserver to send response to client.
         */
        super.executeTransaction(request, responseObserver);
    }
}
