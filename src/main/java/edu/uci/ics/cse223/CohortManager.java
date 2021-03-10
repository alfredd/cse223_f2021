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

    public Twopc.SQL sendPrepare(final Twopc.SQL request) {
        Twopc.SQL finalResponse = null;
        for (Map.Entry<Integer, CohortClient>client:cohortClientMap.entrySet()) {
            Integer cohortID = client.getKey();
            db.insertProtocolLog(request.getId(), cohortID, Twopc.Status.PREPARE.toString());
            Twopc.SQL response = client.getValue().prepare(request);
            db.updateProtocolLog(request.getId(), cohortID, response.getStatus().toString());
            if (response.getStatus()== Twopc.Status.ABORT) {
                finalResponse = Twopc.SQL.newBuilder(response).build();
            }
        }
        if (finalResponse==null) {
            finalResponse = Twopc.SQL.newBuilder().setStatus(Twopc.Status.COMMIT).setId(request.getId()).build();
        }

        return finalResponse;
    }

    public Twopc.SQL sendCommit(final Twopc.SQL request) {
        Twopc.SQL finalResponse = null;
        for (Map.Entry<Integer, CohortClient>client:cohortClientMap.entrySet()) {
            Integer cohortID = client.getKey();
            db.updateProtocolLog(request.getId(), cohortID, Twopc.Status.COMMITTED.toString());
            Twopc.SQL response = client.getValue().commit(request);
            db.updateProtocolLog(response.getId(), cohortID, response.getStatus().toString());
        }
        finalResponse = Twopc.SQL.newBuilder().setStatus(Twopc.Status.COMMITTED).setId(request.getId()).build();

        return finalResponse;
    }

    public Twopc.SQL sendAbort(final Twopc.SQL request) {
        Twopc.SQL finalResponse = null;
        for (Map.Entry<Integer, CohortClient>client:cohortClientMap.entrySet()) {
            Integer cohortID = client.getKey();
            db.updateProtocolLog(request.getId(), cohortID, Twopc.Status.ABORT.toString());
            Twopc.SQL response = client.getValue().abort(request);
            db.updateProtocolLog(response.getId(), cohortID, response.getStatus().toString());
        }
        finalResponse = Twopc.SQL.newBuilder().setStatus(Twopc.Status.ABORTED).setId(request.getId()).build();

        return finalResponse;
    }

    public void setDB(DB db) {
        this.db=db;
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
