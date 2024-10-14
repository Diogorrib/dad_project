package dadkvs.server;

import dadkvs.DadkvsMain;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;

public class MainLoop implements Runnable {
    DadkvsServerState server_state;
    int timestamp;  // for key value store

    private boolean has_work;
    private int next_to_process;    //next index to be processed


    public MainLoop(DadkvsServerState state) {
        this.server_state = state;
        this.has_work = false;
        this.next_to_process = 0;
        this.timestamp = 0;
    }

    public void run() {
        while (true) {
            this.doWork();
            if (this.server_state.debug_mode == 1) {
                System.out.println("DEBUG MODE 1");
                DadkvsServer.simulateCrash();
                break;
            } else if (this.server_state.debug_mode == 2) {
                this.server_state.freeze.enabled = true;
            } else if (this.server_state.debug_mode == 3) {
                this.server_state.freeze.enabled = false;
                this.server_state.freeze.wakeup();
            }
        }
    }


    synchronized public void doWork() {
        System.out.println("Main loop do work start");

        Integer reqid = this.server_state.pendingRequestsForProcessing.get(next_to_process);
        if (reqid != null && this.server_state.pendingRequestsData.get(reqid) != null) {
            replyToClient(reqid, next_to_process);
        }

        this.has_work = false;
        while (!this.has_work) {
            System.out.println("Main loop do work: waiting");
            try {
                wait();
            } catch (InterruptedException _) {
            }
        }
        System.out.println("Main loop do work finish");
    }

    synchronized public void wakeup() {
        this.has_work = true;
        notify();
    }

    private void replyToClient(int reqid, int index) {
        ArrayList<Integer> request = this.server_state.pendingRequestsData.get(reqid);
        if (request.size() == 1) {
            readReply(reqid, index, request);
        } else {
            commitReply(reqid, index, request);
        }
    }

    public void readReply(int reqid, int index, ArrayList<Integer> readElements) {
        int key = readElements.get(0);

        VersionedValue vv = this.server_state.store.read(key);

        // ensure that request was processed before proceeding to the next one
        finishRequestProcess(reqid, index);

        DadkvsMain.ReadReply response = DadkvsMain.ReadReply.newBuilder()
                .setReqid(reqid).setValue(vv.getValue()).setTimestamp(vv.getVersion()).build();

        StreamObserver<DadkvsMain.ReadReply> responseObserver = this.server_state.pendingRequestsReadObserver.get(reqid);
        this.server_state.pendingRequestsReadObserver.remove(reqid);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void commitReply(int reqid, int index, ArrayList<Integer> commitElements) {
        int key1 = commitElements.get(0);
        int version1 = commitElements.get(1);
        int key2 = commitElements.get(2);
        int version2 = commitElements.get(3);
        int writekey = commitElements.get(4);
        int writeval = commitElements.get(5);

        this.timestamp++;
        TransactionRecord txrecord = new TransactionRecord(key1, version1, key2, version2, writekey, writeval, this.timestamp);
        boolean result = this.server_state.store.commit(txrecord);


        // for debug purposes
        System.out.println("Result is ready for request with reqid " + reqid);

        // ensure that request was processed before proceeding to the next one
        finishRequestProcess(reqid, index);

        DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
                .setReqid(reqid).setAck(result).build();

        StreamObserver<DadkvsMain.CommitReply> responseObserver = this.server_state.pendingRequestsCommitObserver.get(reqid);
        this.server_state.pendingRequestsCommitObserver.remove(reqid);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void finishRequestProcess(int reqid, int index) {
        this.server_state.pendingRequestsData.remove(reqid);
        this.server_state.pendingRequestsForProcessing.remove(index);
        this.next_to_process++;
        if (!this.server_state.pendingRequestsForProcessing.isEmpty()) {
            wakeup();
        }
    }
}
