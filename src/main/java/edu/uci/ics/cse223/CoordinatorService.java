package edu.uci.ics.cse223;

import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.TimerTask;

public class CoordinatorService extends CoordinatorGrpc.CoordinatorImplBase {

    private final DB db;
    private final ConfigurationManager cm;

    public CoordinatorService(DB db) throws IOException {
        this.db = db;
        cm = new ConfigurationManager();
        TimerTask timerTask;
    }

    @Override
    public void preparedAck(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        /**
         * Update Cohort ID into the DB log for prepared Txn.
         * Respond with WAIT if response from other cohorts is still needed.
         * Respond with COMMIT if Commit decision has been made.
         */

        String txnid = request.getId();
        int cohortID = request.getAgentID();
        db.updateProtocolLog(txnid, cohortID, request.getStatus().toString());
        TxnState txnState = db.getTxnStatusFromProtocolLog(txnid);

        Twopc.Status txnFinalStatus = Twopc.Status.COMMITTED;
        if (txnState.status != Twopc.Status.COMMIT)
            txnFinalStatus = txnState.status;
        sendMessageToCohorts(request, txnid, cohortID, txnState, txnFinalStatus);
        responseObserver.onNext(Twopc.SQL.newBuilder(request).setStatus(txnState.status).build());
        responseObserver.onCompleted();
    }

    @Override
    public void abortAck(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        /**
         * Update log with Abort message
         * respond with ABORT.
         */
        String txnid = request.getId();
        int cohortID = request.getAgentID();
        db.updateProtocolLog(txnid, cohortID, Twopc.Status.ABORTED.toString());
        Twopc.Status txnFinalStatus = Twopc.Status.ABORTED;
        responseObserver.onNext(Twopc.SQL.newBuilder(request).setStatus(txnFinalStatus).build());
        responseObserver.onCompleted();
    }

    private void sendMessageToCohorts(Twopc.SQL request, String txnid, int cohortID, TxnState txnState, Twopc.Status txnFinalStatus) {
        for (TxnResp resp : txnState.txnResps) {
            db.updateProtocolLog(txnid, resp.cohortID, txnFinalStatus.toString());
            if (resp.cohortID != cohortID) {
                /**
                 * send decision to other cohorts.
                 */
                CohortClient client = new CohortClient(cm.getHostPort(cohortID));
                client.commit(request);
                try {
                    client.shutdown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void commitAck(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        /**
         * Update log with commit message.
         * No response needs to be sent back. Respond with an empty message so that responseObserver does not block at the Cohort.
         */
        db.updateProtocolLog(request.getId(), request.getAgentID(), Twopc.Status.COMMITTED.toString());
        responseObserver.onNext(request);
        responseObserver.onCompleted();
    }
}
