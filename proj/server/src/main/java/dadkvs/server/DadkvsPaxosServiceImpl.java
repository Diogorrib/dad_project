
package dadkvs.server;


import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;

public class DadkvsPaxosServiceImpl extends DadkvsPaxosServiceGrpc.DadkvsPaxosServiceImplBase {

    DadkvsServerState server_state;
    PaxosLoop loop;
    int n_servers;


    public DadkvsPaxosServiceImpl(DadkvsServerState state) {
        this.server_state = state;
        this.loop = state.paxos_loop;
        this.n_servers = DadkvsServerState.n_servers;
    }


    @Override
    public void phaseone(DadkvsPaxos.PhaseOneRequest request, StreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver) {
        // for debug purposes
        System.out.println("Receive phase1 request: " + request);

        int phase1config = request.getPhase1Config();
        int phase1index = request.getPhase1Index();
        int phase1timestamp = request.getPhase1Timestamp();
        DadkvsPaxos.PhaseOneReply response;

        if (phase1timestamp >= this.loop.last_seen_timestamp) {


            this.loop.last_seen_timestamp = phase1timestamp;
            // for debug purposes
            System.out.println("Phase1 accepted for index " + phase1index + " and timestamp " + phase1timestamp);

            response = DadkvsPaxos.PhaseOneReply.newBuilder()
                    .setPhase1Config(phase1config)
                    .setPhase1Index(phase1index)
                    .setPhase1Accepted(true)
                    .setPhase1Value(this.loop.last_seen_value.getValue())
                    .setPhase1Timestamp(this.loop.last_seen_value.getVersion())
                    .build();
        } else {
            // for debug purposes
            System.out.println("Phase1 rejected for index " + phase1index + " and timestamp " + phase1timestamp);

            response = DadkvsPaxos.PhaseOneReply.newBuilder()
                    .setPhase1Config(phase1config)
                    .setPhase1Index(phase1index)
                    .setPhase1Accepted(false)
                    .setPhase1Timestamp(this.loop.last_seen_timestamp)
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DadkvsPaxos.PhaseTwoRequest request, StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {
        int phase2config = request.getPhase2Config();
        int phase2index = request.getPhase2Index();
        int phase2value = request.getPhase2Value();
        int phase2timestamp = request.getPhase2Timestamp();
        DadkvsPaxos.PhaseTwoReply response;

        // for debug purposes
        System.out.println("Receive phase two request: " + request);

        if (phase2timestamp >= this.loop.last_seen_timestamp) {
            this.server_state.updateValue(phase2value, phase2timestamp);

            // for debug purposes
            System.out.println("Phase2 value " + phase2value + " accepted for index " + phase2index + " and timestamp " + phase2timestamp);

            response = DadkvsPaxos.PhaseTwoReply.newBuilder()
                    .setPhase2Config(phase2config)
                    .setPhase2Index(phase2index)
                    .setPhase2Accepted(true)
                    .build();

            Context.current().fork().run(() -> {
                send4Learners(phase2timestamp, phase2index);
            });

        } else {

            // for debug purposes
            System.out.println("Phase2 rejected for index " + phase2index + " and timestamp " + phase2timestamp);

            response = DadkvsPaxos.PhaseTwoReply.newBuilder()
                    .setPhase2Config(phase2config)
                    .setPhase2Index(phase2index)
                    .setPhase2Accepted(false)
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void learn(DadkvsPaxos.LearnRequest request, StreamObserver<DadkvsPaxos.LearnReply> responseObserver) {
        // For debug purposes
        System.out.println("Receive learn request: " + request);

        int learnconfig = request.getLearnconfig();
        int learnindex = request.getLearnindex();
        int learnvalue = request.getLearnvalue();
        int learntimestamp = request.getLearntimestamp(); //!FIXME: why do we need the timestamp here??

        // Save request to be processed in case a Majority of servers accepted this request
        if (learnindex == this.loop.learn_messages_received.get(2)) {

            if (learntimestamp > this.loop.learn_messages_received.get(1)) {
                this.loop.learn_messages_received.set(0, 1);
                this.loop.learn_messages_received.set(1, learntimestamp);

            } else if (learntimestamp == this.loop.learn_messages_received.get(1)) {
                this.loop.learn_messages_received.set(0, this.loop.learn_messages_received.getFirst() + 1);

                if (this.loop.learn_messages_received.getFirst() == 2) { //!FIXME: should be 2 or 3??
                   this.server_state.pendingRequestsForProcessing.put(learnvalue, learnindex);
                   this.server_state.pendingRequestsForPaxos.remove("" + learnvalue);
                   this.server_state.orderedRequestsByPaxos.put(learnvalue, learnindex);
                   this.server_state.resetPaxosInstance();
               }
            }
        }

        // For debug purposes
        System.out.println("Learn value " + learnvalue + " for index " + learnindex + " and timestamp " + learntimestamp);

        DadkvsPaxos.LearnReply response = DadkvsPaxos.LearnReply.newBuilder() //!FIXME: Always true???
                .setLearnconfig(learnconfig)
                .setLearnindex(learnindex)
                .setLearnaccepted(true)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // Acceptor when accepts Phase2 sends ACCEPT to all learners
    private void send4Learners(int timestamp, int index) {
        DadkvsPaxos.LearnRequest.Builder learn_request = DadkvsPaxos.LearnRequest.newBuilder();

        learn_request.setLearnconfig(this.server_state.configuration)
                .setLearnindex(index)
                .setLearnvalue(this.loop.last_seen_value.getValue())
                .setLearntimestamp(timestamp);

        // Send request
        ArrayList<DadkvsPaxos.LearnReply> learn_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.LearnReply> learn_collector
                = new GenericResponseCollector<>(learn_responses, n_servers);

        // For debug purposes
        System.out.println("LEARN sending request to all learners for index: " + index + " and timestamp: " + timestamp);

        // Request is sent for learners (every server)
        for (int i = 0; i < n_servers; i++) {
            CollectorStreamObserver<DadkvsPaxos.LearnReply> learn_observer
                    = new CollectorStreamObserver<>(learn_collector);
            this.server_state.async_stubs[i].learn(learn_request.build(), learn_observer);
        }
    }
}
