package dadkvs.server;

import java.util.ArrayList;
import java.util.Random;

/* these imported classes are generated by the contract */

import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;
import dadkvs.DadkvsSequencer;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.stub.StreamObserver;

public class DadkvsMainServiceImpl extends DadkvsMainServiceGrpc.DadkvsMainServiceImplBase {

    DadkvsServerState server_state;
    int timestamp;
    boolean freezeEnabled;
    boolean delayEnabled;

    public DadkvsMainServiceImpl(DadkvsServerState state) {
        this.server_state = state;
        this.timestamp = 0;
    }

    @Override
    public void read(DadkvsMain.ReadRequest request, StreamObserver<DadkvsMain.ReadReply> responseObserver) {
        if (freeze()) {
            System.out.println("freezed blocking read request:" + request);
            return;
        }
        // for debug purposes
        System.out.println("Receiving read request:" + request);

        int reqid = request.getReqid();
        int key = request.getKey();
        getOrder(reqid);
        delay();

        VersionedValue vv = this.server_state.store.read(key);

        // ensure that request was processed before proceeding to the next one
        finishRequestProcess();

        DadkvsMain.ReadReply response = DadkvsMain.ReadReply.newBuilder()
                .setReqid(reqid).setValue(vv.getValue()).setTimestamp(vv.getVersion()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void committx(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
        if (freeze()) {
            System.out.println("freezed blocking commit request:" + request);
            return;
        }
        // for debug purposes
        System.out.println("Receiving commit request:" + request);

        int reqid = request.getReqid();
        int key1 = request.getKey1();
        int version1 = request.getVersion1();
        int key2 = request.getKey2();
        int version2 = request.getVersion2();
        int writekey = request.getWritekey();
        int writeval = request.getWriteval();
        getOrder(reqid);
        delay();

        // for debug purposes
        System.out.println("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2 + " v2 " + version2 + " wk " + writekey + " writeval " + writeval);

        this.timestamp++;
        TransactionRecord txrecord = new TransactionRecord(key1, version1, key2, version2, writekey, writeval, this.timestamp);
        boolean result = this.server_state.store.commit(txrecord);

        // ensure that request was processed before proceeding to the next one
        finishRequestProcess();

        // for debug purposes
        System.out.println("Result is ready for request with reqid " + reqid);

        DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
                .setReqid(reqid).setAck(result).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private boolean freeze() {
        if (this.server_state.debug_mode == 2) {
            freezeEnabled = true;
        } else if (this.server_state.debug_mode == 3) {
            freezeEnabled = false;
        }
        return freezeEnabled;
    }

    private void delay() {
        if (this.server_state.debug_mode == 4) {
            delayEnabled = true;
        } else if (this.server_state.debug_mode == 5) {
            delayEnabled = false;
        }

        if (delayEnabled) {
            Random random = new Random();
            int randomDelay = 100 + random.nextInt(9900);

            // for debug purposes
            System.out.println("delaying " + randomDelay + " milliseconds");
            try {
                Thread.sleep(randomDelay);
            } catch (InterruptedException e) {
            }
        }
    }

    private void getOrder(int reqId) {
        if (this.server_state.i_am_leader) {
            sendOrder(reqId);
        }
        // the leader also waits since it also receives the order from itself
        this.server_state.waitForOrder(reqId);
    }

    private void sendOrder(int reqId) {
        DadkvsSequencer.SendSeqNumberRequest.Builder sequence_number_request = DadkvsSequencer.SendSeqNumberRequest.newBuilder();

        sequence_number_request.setReqid(reqId)
                .setSeqNumber(this.server_state.sequence_number);

        // for debug purposes
        System.out.println("Sent to all servers seqNumber: " +  this.server_state.sequence_number + " assigned to reqId: " + reqId);

        this.server_state.sequence_number++;

        //Send request
        ArrayList<DadkvsSequencer.SendSeqNumberReply> sequence_number_responses = new ArrayList<DadkvsSequencer.SendSeqNumberReply>();
        GenericResponseCollector<DadkvsSequencer.SendSeqNumberReply> sequence_number_collector
                = new GenericResponseCollector<DadkvsSequencer.SendSeqNumberReply>(sequence_number_responses, 5);

        for (int i = 0; i < 5; i++) {
            CollectorStreamObserver<DadkvsSequencer.SendSeqNumberReply> sequence_number_observer = new CollectorStreamObserver<DadkvsSequencer.SendSeqNumberReply>(sequence_number_collector);
            this.server_state.async_stubs[i].sendseqnumber(sequence_number_request.build(), sequence_number_observer);
        }
        sequence_number_collector.waitForTarget(1);

        // for debug purposes
        System.out.println("Received acks from all servers");
    }

    private void finishRequestProcess() {
        this.server_state.curr_seq_number++;
        if (!this.server_state.pendingRequests.isEmpty()) {
            this.server_state.wakeUp();
        }
    }
}
