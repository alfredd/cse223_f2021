package edu.uci.ics.cse223;

import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
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
                1, 10, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        cm = new CohortManager();
        cm.setDB(db);
    }

    public void checkBackupTxns() {
        /**
         * Load Status of txns from LOG.
         * If PREPARE but no response from Cohorts yet:
         *      Send PREPARE again.
         * If COMMIT: Send COMMIT to cohorts again
         *
         * Finally remove COMMITTED logs from DB.
         *
         */

    }

    @Override
    public void executeTransaction(Twopc.Transaction request, StreamObserver<Twopc.TransactionStatus> responseObserver) {
        /**
         * To handle individual transactions, create individual threads. ThreadLooper.
         * Log incoming transactions.
         * Call Cohorts with the SQL statement to prepare the transaction.
         * Based on Response from all Cohorts, Commit OR Abort.
         * Finally call responseObserver to send response to client.
         */
        threadPoolExecutor.execute(new TransactionExecutor(request, responseObserver, db, cm));
    }
}


class TransactionExecutor implements Runnable {
    private final Twopc.Transaction request;
    private final StreamObserver<Twopc.TransactionStatus> responseObserver;
    private final DB db;
    private final CohortManager cm;

    public TransactionExecutor(Twopc.Transaction request, StreamObserver<Twopc.TransactionStatus> responseObserver, DB db, CohortManager cm) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.db = db;
        this.cm = cm;
    }


    @Override
    public void run() {
        /**
         * Add Forced Log For transactions:
         * Add Entries for Prepare statement into DB table
         * 1. TxnID, TxnStmts
         * 2. TxnID, CohortID, Status (PREPARE, COMMIT)
         */
        List<Twopc.HashedQuery> queries = request.getStatementList();

//        String txnQuery = String.join(";", request.getStatementList());
        db.insertRedoLog(request.getId(), queries);
        TxnState responseState = cm.sendPrepare(request);
        if (responseState.status == Twopc.Status.COMMIT) {
            for (TxnResp txnQueries : responseState.txnResps) {
                db.updateProtocolLog(responseState.txnID, txnQueries.cohortID, Twopc.Status.COMMITTED.toString());
            }
            TxnState commitResponse = cm.sendCommit(responseState);
            /**
             * Delete transaction entries from log.
             */
            responseObserver.onNext(Twopc.TransactionStatus.newBuilder().setStatus(Twopc.Status.COMMITTED).build());
            responseObserver.onCompleted();
        }
        if (responseState.status == Twopc.Status.WAIT) {
            for (TxnResp txnQueries: responseState.txnResps) {
                db.updateProtocolLog(responseState.txnID, txnQueries.cohortID, responseState.status.toString());
            }
        } else {
            /**
             * Update Txn Entries to ABORT.
             */
            for (TxnResp txnQueries : responseState.txnResps) {
                db.updateProtocolLog(responseState.txnID, txnQueries.cohortID, Twopc.Status.ABORTED.toString());
            }
            TxnState abortResponse = cm.sendAbort(responseState);
            responseObserver.onNext(Twopc.TransactionStatus.newBuilder().setStatus(Twopc.Status.ABORTED).build());
            responseObserver.onCompleted();
        }
    }
}