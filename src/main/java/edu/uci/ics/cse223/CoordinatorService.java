package edu.uci.ics.cse223;

import io.grpc.stub.StreamObserver;

public class CoordinatorService extends CoordinatorGrpc.CoordinatorImplBase {

    private final DB db;

    public CoordinatorService(DB db) {
        this.db = db;
    }

    @Override
    public void preparedAck(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        /**
         * Update Cohort ID into the DB log for prepared Txn.
         * Respond with WAIT if response from other cohorts is still needed.
         * Respond with COMMIT if Commit decision has been made.
         */
        super.preparedAck(request, responseObserver);
    }

    @Override
    public void abortAck(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        /**
         * Update log with Abort message
         * respond with ABORT.
         */
        super.abortAck(request, responseObserver);
    }

    @Override
    public void commitAck(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        /**
         * Update log with commit message.
         * No response needs to be sent back. Respond with an empty message so that responseObserver does not block at the Cohort.
         */
        super.commitAck(request, responseObserver);
    }
}
