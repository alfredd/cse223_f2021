package edu.uci.ics.cse223;

import io.grpc.stub.StreamObserver;

public class CohortService extends CohortGrpc.CohortImplBase {
    private final DB db;

    public CohortService(DB db) {
        this.db=db;
    }

    @Override
    public void prepare(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        super.prepare(request, responseObserver);
    }

    @Override
    public void commit(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        super.commit(request, responseObserver);
    }

    @Override
    public void abort(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        super.abort(request, responseObserver);
    }
}
