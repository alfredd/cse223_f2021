package edu.uci.ics.cse223;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class CohortService extends CohortGrpc.CohortImplBase {
    private final DB db;
    private final CoordinatorClient coordinatorClient;
    private final Integer id;


    public CohortService(Integer cohortID, DB db) throws IOException {
        this.db = db;
        coordinatorClient = new CoordinatorClient();
        this.id = cohortID;
    }

    public void checkBackupTxns() {
        /**
         * Load TxnID and status from the Cohort Table.
         * if Status== PREPARED
         * Send COMMIT to Coordinator (PrepareAck)
         * if status == COMMIT
         * Send COMMIT Ack to Coordinator
         *
         * if status == ABORT
         * send AbortAck to Coordinator.
         */
    }

    @Override
    public void prepare(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        /**
         * LOG incoming transaction with txnID, status.
         * DB call to PREPARE TRANSACTION with request.id
         * Based on return value add response to responseObserver.
         */

        Scanner scanner = new Scanner(System.in);
        System.out.println("1 to Prepare, Anything else to abort.");
        String choice = scanner.next();
        Twopc.Status responseStatus = Twopc.Status.COMMIT;
        if (!choice.equals("1")) {
            responseStatus = Twopc.Status.ABORT;
            db.insertCohortLog(request.getId(), Twopc.Status.ABORT.toString());
        }
        if (responseStatus == Twopc.Status.COMMIT) {
            db.insertCohortLog(request.getId(), Twopc.Status.PREPARE.toString());
            boolean status = db.prepareSQL(request);
            if (status) {
                db.updateCohortLog(request.getId(), Twopc.Status.COMMIT.toString());
                responseStatus = Twopc.Status.COMMIT;
            } else {
                db.updateCohortLog(request.getId(), Twopc.Status.ABORT.toString());
                responseStatus = Twopc.Status.ABORT;
            }
        }
        Twopc.SQL resp = Twopc.SQL.newBuilder().setId(request.getId()).setAgentID(this.id).setStatus(responseStatus).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void commit(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        db.updateCohortLog(request.getId(), Twopc.Status.COMMIT.toString());
        boolean status = db.commitSQL(request);
        Twopc.Status responseStatus;
        if (status) {
            db.updateCohortLog(request.getId(), Twopc.Status.COMMITTED.toString());
        } else {
            System.out.println("Commit PREPARED failed.");
        }
        responseStatus = Twopc.Status.DONE;
        Twopc.SQL resp = Twopc.SQL.newBuilder().setId(request.getId()).setAgentID(this.id).setStatus(responseStatus).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }

    @Override
    public void abort(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        db.updateCohortLog(request.getId(), Twopc.Status.ABORT.toString());
        boolean status = db.abortSQL(request);
        Twopc.Status responseStatus;
        if (status) {
            db.updateCohortLog(request.getId(), Twopc.Status.ABORTED.toString());
        } else {
            System.out.println("DB ROLLBACK failed.");
        }
        responseStatus = Twopc.Status.ABORTED;
        Twopc.SQL resp = Twopc.SQL.newBuilder().setId(request.getId()).setAgentID(this.id).setStatus(responseStatus).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
    }
}


class CoordinatorClient {
    private final ManagedChannel channel;
    private int port;
    private CoordinatorGrpc.CoordinatorBlockingStub blockingStub;

    public CoordinatorClient() throws IOException {
        ConfigurationManager cm = new ConfigurationManager();
        this.port = cm.getHostPort(0);
        this.channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        blockingStub = CoordinatorGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public boolean isConnected() {
        return !channel.isTerminated();
    }

    public void sendPrepareAck(String id) {

        blockingStub.preparedAck(Twopc.SQL.newBuilder().setId(id).build());
        /**
         * Set status in log to PREPARED
         */
    }

    public void sendCommitAck(String id) {
        blockingStub.commitAck(Twopc.SQL.newBuilder().setId(id).build());
        /**
         * Set status in log to COMMITTED
         */
    }

    public void sendAbortAck(String id) {
        blockingStub.abortAck(Twopc.SQL.newBuilder().setId(id).build());
        /**
         * Set status in log to ABORTED.
         */
    }


}