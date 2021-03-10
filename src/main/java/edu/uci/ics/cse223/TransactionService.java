package edu.uci.ics.cse223;

import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TransactionService extends TransactionServiceGrpc.TransactionServiceImplBase {

    private final DB db;
    private final ThreadPoolExecutor threadPoolExecutor;
    private CohortManager cm;

    public TransactionService(DB db) throws IOException {
        this.db = db;
        threadPoolExecutor = new ThreadPoolExecutor(
                1, 1, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        cm = new CohortManager();
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
        threadPoolExecutor.execute(new TransactionExecutor(request,responseObserver,db,cm));
    }
}


class TransactionExecutor implements Runnable {
    private final Twopc.SQL request;
    private final StreamObserver<Twopc.TransactionStatus> responseObserver;
    private final DB db;
    private final CohortManager cm;

    public TransactionExecutor(Twopc.SQL request, StreamObserver<Twopc.TransactionStatus> responseObserver, DB db, CohortManager cm) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.db = db;
        this.cm = cm;
    }


    @Override
    public void run() {
        Twopc.SQL prepareResponse = cm.sendPrepare(request);
        if (prepareResponse.getStatus()== Twopc.Status.COMMIT) {
            Twopc.SQL commitResponse = cm.sendCommit(prepareResponse);
            responseObserver.onNext(Twopc.TransactionStatus.newBuilder().setStatus(Twopc.Status.COMMITTED).build());
            responseObserver.onCompleted();
        } else {
            Twopc.SQL abortResponse = cm.sendAbort(Twopc.SQL.newBuilder(prepareResponse).setStatus(Twopc.Status.ABORT).build());
            responseObserver.onNext(Twopc.TransactionStatus.newBuilder().setStatus(Twopc.Status.ABORTED).build());
            responseObserver.onCompleted();
        }
    }
}