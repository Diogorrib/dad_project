package dadkvs.server;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;
import dadkvs.DadkvsPaxos;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

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

        paxos();
        delay();

        VersionedValue vv = this.server_state.store.read(key);

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

        paxos();
        delay();

        // for debug purposes
        System.out.println("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2 + " v2 " + version2 + " wk " + writekey + " writeval " + writeval);

        this.timestamp++;
        TransactionRecord txrecord = new TransactionRecord(key1, version1, key2, version2, writekey, writeval, this.timestamp);
        boolean result = this.server_state.store.commit(txrecord);

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

    private void paxos() {
        // wait here for previous paxos instance to be completed

        // config     = 0 for now (relevant for step3)
        // index      = order that is being agreed
        // value      = reqId that is being agreed for index
        // timestamp  = value % 5 (== server id)

        if (this.server_state.i_am_leader && this.server_state.inConfiguration()) {
            boolean phaseone_completed = false;
            while (!phaseone_completed) {
                phaseone_completed = phase1();
            }
            // for debug purposes
            System.out.println("Phase1 completed");
        }

        // TODO
        // if (leader i.e. phase1 accepted)
        // phase2

        // if (acceptor i.e. {0, 1, 2} and value agreed in phase2)
        // learn

        // unlock here after consensus
    }

    private boolean phase1() {
        int responses_needed = 2;
        DadkvsPaxos.PhaseOneRequest.Builder sequence_number_request = DadkvsPaxos.PhaseOneRequest.newBuilder();

        sequence_number_request.setPhase1Config(this.server_state.configuration)
                .setPhase1Index(this.server_state.curr_index)
                .setPhase1Timestamp(this.server_state.timestamp);

        //Send request
        ArrayList<DadkvsPaxos.PhaseOneReply> phaseone_responses = new ArrayList<DadkvsPaxos.PhaseOneReply>();
        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseone_collector
                = new GenericResponseCollector<DadkvsPaxos.PhaseOneReply>(phaseone_responses, 3);

        // for debug purposes
        System.out.println("Phase1 sending request to all acceptors for index: " + this.server_state.curr_index + " and timestamp: " + this.server_state.timestamp);

        for (int i = this.server_state.configuration; i < this.server_state.configuration + 3; i++) {
            CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phaseone_observer
                    = new CollectorStreamObserver<DadkvsPaxos.PhaseOneReply>(phaseone_collector);
            this.server_state.async_stubs[i].phaseone(sequence_number_request.build(), phaseone_observer);
        }

        phaseone_collector.waitForTarget(responses_needed);

        if (phaseone_responses.size() >= responses_needed) {
            Iterator<DadkvsPaxos.PhaseOneReply> phaseone_iterator = phaseone_responses.iterator();
            for (int i = 0; i < responses_needed; i++) {
                DadkvsPaxos.PhaseOneReply phaseone_reply = phaseone_iterator.next();
                int timestamp = phaseone_reply.getPhase1Timestamp();
                if (!phaseone_reply.getPhase1Accepted()) {
                    this.server_state.timestamp = timestamp / 5 + this.server_state.my_id;
                    // for debug purposes
                    System.out.println("Phase1 leader rejected. Increase timestamp to: " + this.server_state.timestamp);
                    return false; // try again with new timestamp
                }
                if (timestamp > this.server_state.last_seen_value.getVersion()) {
                    int value = phaseone_reply.getPhase1Value();
                    // for debug purposes
                    System.out.println("Phase1 already accepted value: " + value + " with timestamp: " + timestamp);
                    this.server_state.last_seen_value.setVersion(timestamp);
                    this.server_state.last_seen_value.setValue(value);
                }
            }
            return true;
        } else {
            System.out.println("Phase1 ERROR");
            return false;
        }
    }

    private void phase2() {
        // TODO
    }
    private void learn() {
        // TODO
    }
}
