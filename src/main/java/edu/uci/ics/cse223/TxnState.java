package edu.uci.ics.cse223;


import java.util.LinkedList;
import java.util.List;

public class TxnState {
    public List<TxnResp> txnResps = new LinkedList<>();
    public Twopc.Status status;
    public String txnID;
    public void addTxnState(int cohortID, Twopc.Status status) {
        txnResps.add(new TxnResp(cohortID, status));
    }
}
class TxnResp {

    public int cohortID;
    public Twopc.Status status;

    public TxnResp( int cohortID, Twopc.Status status) {

        this.cohortID = cohortID;
        this.status = status;
    }
}