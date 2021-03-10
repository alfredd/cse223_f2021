package edu.uci.ics.cse223;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class CohortService extends CohortGrpc.CohortImplBase {
    private final DB db;
    private final CoordinatorClient coordinatorClient;

    public CohortService(DB db) throws IOException {
        this.db=db;
        coordinatorClient = new CoordinatorClient();

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
        //boolean = DB.prepareSQL(request);
        Twopc.SQL resp = Twopc.SQL.newBuilder().setStatus(Twopc.Status.ABORT).build();
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
//        super.prepare(request, responseObserver);
    }

    @Override
    public void commit(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        //boolean = DB.commitSQL(request);
        super.commit(request, responseObserver);
    }

    @Override
    public void abort(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        //boolean = DB.abortSQL(request);
        super.abort(request, responseObserver);
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