package dadkvs.server;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;
import dadkvs.DadkvsPaxos;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.stub.StreamObserver;
import io.grpc.Context;

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

        splitThreads(reqid);
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

        splitThreads(reqid);
        delay();

        // for debug purposes
        System.out.println("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2 + " v2 " + version2 + " wk " + writekey + " writeval " + writeval);

        this.timestamp++;
        TransactionRecord txrecord = new TransactionRecord(key1, version1, key2, version2, writekey, writeval, this.timestamp);
        boolean result = this.server_state.store.commit(txrecord);

        //Update configuration attribute on server_state
        if (writekey == 0 && result) {
            this.server_state.configuration = writeval;
        }

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

    private void splitThreads(int reqid) {
        Context.current().fork().run(() -> {
            paxos(reqid);
        });
        waitForOrder(reqid);
    }

    private synchronized void paxos(int reqId) {

        this.server_state.pendingRequestsForPaxos.add(reqId);
        boolean in_paxos = this.server_state.in_paxos_instance;
        while (in_paxos) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
            in_paxos = this.server_state.in_paxos_instance;
        }
        this.server_state.in_paxos_instance = true;

        //I'm a proposer
        if (this.server_state.i_am_leader && this.server_state.inConfiguration()) {
            boolean phasetwo_completed = false;

            while (!phasetwo_completed) {
                boolean phaseone_completed = false;
                BackOff backoff = new BackOff();
                while (!phaseone_completed) {
                    phaseone_completed = phase1();
                    // wait before trying with higher leader value
                    if(!phaseone_completed)
                        waitBackoff(backoff);
                }
                // for debug purposes
                System.out.println("Phase1 completed");

                phasetwo_completed = phase2();
            }
            // for debug purposes
            System.out.println("Phase2 completed");
        }

        // All are learners -> wait until majority of learnRequests  have arrived
        int learn_messages_received = this.server_state.learn_messages_received.getValue();
        while (learn_messages_received < 3) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
            learn_messages_received = this.server_state.learn_messages_received.getValue();
        }

    }

    synchronized public void waitForOrder(int reqId) {
        Integer seq_number = this.server_state.pendingRequestsForProcessing.get(reqId);
        while (seq_number == null || seq_number != curr_seq_number) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
            seq_number = pendingRequests.get(reqId);
        }
        System.out.println("Wait for Order: completed");
        pendingRequests.remove(reqId);
    }

    private void waitBackoff(BackOff backoff) {
        long backoff_delay = backoff.calculateBackoffDelay();
        // for debug purposes
        System.out.println("Back off: " + backoff_delay + " milliseconds");
        try {
            Thread.sleep(backoff_delay);
        } catch (InterruptedException e) {
        }
    }

    private boolean phase1() {
        int n_acceptors = 3;
        int responses_needed = 2; //Majority of acceptors (2 of 3)

        DadkvsPaxos.PhaseOneRequest.Builder phaseone_request = DadkvsPaxos.PhaseOneRequest.newBuilder();

        phaseone_request.setPhase1Config(this.server_state.configuration)
                .setPhase1Index(this.server_state.curr_index)
                .setPhase1Timestamp(this.server_state.timestamp);

        //Send request
        ArrayList<DadkvsPaxos.PhaseOneReply> phaseone_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseone_collector
                = new GenericResponseCollector<>(phaseone_responses, n_acceptors);

        // for debug purposes
        System.out.println("Phase1 sending request to all acceptors for index: " + this.server_state.curr_index + " and timestamp: " + this.server_state.timestamp);

        // Request is only sent for acceptors
        for (int i = this.server_state.configuration; i < this.server_state.configuration + n_acceptors; i++) {
            CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phaseone_observer
                    = new CollectorStreamObserver<>(phaseone_collector);
            this.server_state.async_stubs[i].phaseone(phaseone_request.build(), phaseone_observer);
        }

        phaseone_collector.waitForTarget(responses_needed);

        //Got a majority of responses
        if (phaseone_responses.size() >= responses_needed) {
            Iterator<DadkvsPaxos.PhaseOneReply> phaseone_iterator = phaseone_responses.iterator();
            for (int i = 0; i < responses_needed; i++) {
                DadkvsPaxos.PhaseOneReply phaseone_reply = phaseone_iterator.next();
                int timestamp = phaseone_reply.getPhase1Timestamp();

                //Phase 1 rejected by one of the acceptors
                if (!phaseone_reply.getPhase1Accepted()) {
                    this.server_state.timestamp = (timestamp / 5 + 1) * 5 + this.server_state.my_id;
                    // for debug purposes
                    System.out.println("Phase1 Acceptor rejected. Increase timestamp to: " + this.server_state.timestamp);
                    return false; // try again with new timestamp
                }

                //Got accepted value from previous phase 2, update last_seen_value
                if (timestamp > this.server_state.last_seen_value.getVersion()) {
                    int value = phaseone_reply.getPhase1Value();
                    // for debug purposes
                    System.out.println("Phase1 already accepted value: " + value + " with timestamp: " + timestamp);
                    this.server_state.updateValue(value, timestamp);
                }
            }
            return true;
        } else {
            System.out.println("Phase1 ERROR");
            return false;
        }
    }

    private boolean phase2() {
        int n_acceptors = 3;
        int responses_needed = 2; //Majority of acceptors (2 of 3)

        DadkvsPaxos.PhaseTwoRequest.Builder phasetwo_request = DadkvsPaxos.PhaseTwoRequest.newBuilder();

        int value = this.server_state.last_seen_value.getValue();
        if (value == -1) {
            value = this.server_state.pendingRequestsForPaxos.getFirst();
        }

        this.server_state.updateValue(value, this.server_state.timestamp);

        phasetwo_request.setPhase2Config(this.server_state.configuration)
                .setPhase2Index(this.server_state.curr_index)
                .setPhase2Value(value)
                .setPhase2Timestamp(this.server_state.timestamp);

        //Send request
        ArrayList<DadkvsPaxos.PhaseTwoReply> phasetwo_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phasetwo_collector
                = new GenericResponseCollector<>(phasetwo_responses, n_acceptors);

        // for debug purposes
        System.out.println("Phase2 sending request to all acceptors for index: " + this.server_state.curr_index + " and timestamp: " + this.server_state.timestamp);

        // Request is only sent for acceptors
        for (int i = this.server_state.configuration; i < this.server_state.configuration + n_acceptors; i++) {
            CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phasetwo_observer
                    = new CollectorStreamObserver<>(phasetwo_collector);
            this.server_state.async_stubs[i].phasetwo(phasetwo_request.build(), phasetwo_observer);
        }

        phasetwo_collector.waitForTarget(responses_needed);

        //Got a majority of responses
        if (phasetwo_responses.size() >= responses_needed) {
            Iterator<DadkvsPaxos.PhaseTwoReply> phasetwo_iterator = phasetwo_responses.iterator();
            for (int i = 0; i < responses_needed; i++) {
                DadkvsPaxos.PhaseTwoReply phasetwo_reply = phasetwo_iterator.next();
                // TODO
            }
            return true;
        } else {
            System.out.println("Phase2 ERROR");
            return false;
        }
    }
}
