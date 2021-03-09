package edu.uci.ics.cse223;

import io.grpc.stub.StreamObserver;

public class CoordinatorService extends CoordinatorGrpc.CoordinatorImplBase{

    private final DB db;

    public CoordinatorService(DB db) {
        this.db=db;
    }

    @Override
    public void preparedAck(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        super.preparedAck(request, responseObserver);
    }

    @Override
    public void abortAck(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        super.abortAck(request, responseObserver);
    }

    @Override
    public void commitAck(Twopc.SQL request, StreamObserver<Twopc.SQL> responseObserver) {
        super.commitAck(request, responseObserver);
    }
}
