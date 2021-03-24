package edu.uci.ics.cse223;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CohortManager {
    private Map<Integer, CohortClient> cohortClientMap = new LinkedHashMap<>();
    private DB db;

    public CohortManager() throws IOException {
        ConfigurationManager cm = new ConfigurationManager();

        for (int i = 1; i < 4; i++) {
            cohortClientMap.put(i, new CohortClient(cm.getHostPort(i)));
        }
    }

    public TxnState sendPrepare(final Twopc.Transaction request) {
        Twopc.SQL finalResponse = null;
        TxnState state = new TxnState();
        state.status = Twopc.Status.COMMIT;
        state.txnID = request.getId();
        for (Twopc.HashedQuery hashedQuery : request.getStatementList()) {
            Integer cohortID = hashedQuery.getHash();
            CohortClient client = cohortClientMap.get(cohortID);
            db.insertProtocolLog(request.getId(), cohortID, Twopc.Status.PREPARE.toString());
            Twopc.SQL prepareResponse;
            Twopc.SQL.Builder twoPCrequest = Twopc.SQL.newBuilder()
                    .setId(request.getId())
                    .setAgentID(cohortID)
                    .addStatement(hashedQuery.getStatement());
            try {

                prepareResponse = client.prepare(
                        twoPCrequest.build());
            } catch (Exception e) {
                System.out.println("Cannot get prepare response from CohortID: " + cohortID + ". " + e.getLocalizedMessage());
                prepareResponse = twoPCrequest.setStatus(Twopc.Status.WAIT).build();
            }
            db.updateProtocolLog(request.getId(), cohortID, prepareResponse.getStatus().toString());
            state.addTxnState(cohortID, prepareResponse.getStatus());
            if (prepareResponse.getStatus() != Twopc.Status.COMMIT) {
                if (state.status != Twopc.Status.ABORT)
                    state.status = prepareResponse.getStatus();
            }
        }
/*        for (Map.Entry<Integer, CohortClient> client : cohortClientMap.entrySet()) {
            Integer cohortID = client.getKey();
            db.insertProtocolLog(request.getId(), cohortID, Twopc.Status.PREPARE.toString());
            Twopc.SQL response = client.getValue().prepare(request);
            db.updateProtocolLog(request.getId(), cohortID, response.getStatus().toString());
            if (response.getStatus() == Twopc.Status.ABORT) {
                finalResponse = Twopc.SQL.newBuilder(response).build();
            }
        }*/
/*        if (finalResponse == null) {
            finalResponse = Twopc.SQL.newBuilder().setStatus(Twopc.Status.COMMIT).setId(request.getId()).build();
        }*/

        return state;
    }

    public TxnState sendCommit(final TxnState request) {

        for (TxnResp prepareResponses : request.txnResps) {
            Integer cohortID = prepareResponses.cohortID;
            CohortClient client = cohortClientMap.get(cohortID);
            db.updateProtocolLog(request.txnID, cohortID, Twopc.Status.COMMITTED.toString());
            Twopc.SQL response = client.commit(
                    Twopc.SQL.newBuilder()
                            .setAgentID(cohortID)
                            .setId(request.txnID)
                            .setStatus(Twopc.Status.COMMITTED)
                            .build());
            db.updateProtocolLog(response.getId(), cohortID, response.getStatus().toString());
        }
        return request;
    }

    public TxnState sendAbort(final TxnState request) {

        for (TxnResp prepareResponses : request.txnResps) {
            Integer cohortID = prepareResponses.cohortID;
            CohortClient client = cohortClientMap.get(cohortID);
            db.updateProtocolLog(request.txnID, cohortID, Twopc.Status.ABORT.toString());
            Twopc.SQL response = client.abort(
                    Twopc.SQL.newBuilder()
                            .setAgentID(cohortID)
                            .setId(request.txnID)
                            .setStatus(Twopc.Status.ABORT)
                            .build());
            db.updateProtocolLog(response.getId(), cohortID, response.getStatus().toString());
        }
        return request;
        /*Twopc.SQL finalResponse = null;
        for (Map.Entry<Integer, CohortClient> client : cohortClientMap.entrySet()) {
            Integer cohortID = client.getKey();
            db.updateProtocolLog(request.getId(), cohortID, Twopc.Status.ABORT.toString());
            Twopc.SQL response = client.getValue().abort(request);
            db.updateProtocolLog(response.getId(), cohortID, response.getStatus().toString());
        }
        finalResponse = Twopc.SQL.newBuilder().setStatus(Twopc.Status.ABORTED).setId(request.getId()).build();

        return finalResponse;*/
    }

    public void setDB(DB db) {
        this.db = db;
    }
}

class CohortClient {
    private final ManagedChannel channel;
    private int port;
    private CohortGrpc.CohortBlockingStub blockingStub;

    public CohortClient(int port) {
        this(ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build());
        this.port = port;
    }

    private CohortClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = CohortGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public boolean isConnected() {
        return !channel.isTerminated();
    }

    public Twopc.SQL commit(Twopc.SQL request) {
        return blockingStub.commit(request);
    }

    public Twopc.SQL prepare(Twopc.SQL request) {
        return blockingStub.prepare(request);
    }

    public Twopc.SQL abort(Twopc.SQL request) {
        return blockingStub.abort(request);
    }

}
