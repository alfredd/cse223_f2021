package edu.uci.ics.cse223;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CohortManager {
    private Map<Integer, CohortClient> cohortClientMap = new LinkedHashMap<>();

    public CohortManager() throws IOException {
//        ConfigurationManager cm = new ConfigurationManager();
//        for (int i = 1; i < 4; i++) {
//            cohortClientMap.put(i, new CohortClient())
//        }
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
}
