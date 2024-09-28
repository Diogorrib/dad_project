
package dadkvs.server;


import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.stub.StreamObserver;
import io.grpc.Context;

import java.util.ArrayList;

public class DadkvsPaxosServiceImpl extends DadkvsPaxosServiceGrpc.DadkvsPaxosServiceImplBase {


    DadkvsServerState server_state;


    public DadkvsPaxosServiceImpl(DadkvsServerState state) {
        this.server_state = state;
    }


    @Override
    public void phaseone(DadkvsPaxos.PhaseOneRequest request, StreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver) {
        // for debug purposes
        System.out.println("Receive phase1 request: " + request);

        int phase1config = request.getPhase1Config();
        int phase1index = request.getPhase1Index();
        int phase1timestamp = request.getPhase1Timestamp();
        DadkvsPaxos.PhaseOneReply response;

        if ((!this.server_state.i_am_leader || phase1timestamp % 5 == this.server_state.my_id)
                && phase1timestamp >= this.server_state.last_seen_timestamp) {

            this.server_state.last_seen_timestamp = phase1timestamp;
            // for debug purposes
            System.out.println("Phase1 accepted for index " + phase1index + " and timestamp " + phase1timestamp);

            response = DadkvsPaxos.PhaseOneReply.newBuilder()
                    .setPhase1Config(phase1config)
                    .setPhase1Index(phase1index)
                    .setPhase1Accepted(true)
                    .setPhase1Value(this.server_state.last_seen_value.getValue())
                    .setPhase1Timestamp(this.server_state.last_seen_value.getVersion())
                    .build();
        } else {
            // for debug purposes
            System.out.println("Phase1 rejected for index " + phase1index + " and timestamp " + phase1timestamp);

            response = DadkvsPaxos.PhaseOneReply.newBuilder()
                    .setPhase1Config(phase1config)
                    .setPhase1Index(phase1index)
                    .setPhase1Accepted(false)
                    .setPhase1Timestamp(this.server_state.last_seen_timestamp)
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

        if ((!this.server_state.i_am_leader || phase2timestamp % 5 == this.server_state.my_id)
                && phase2timestamp >= this.server_state.last_seen_timestamp) {
            this.server_state.updateValue(phase2value, phase2timestamp);

            // for debug purposes
            System.out.println("Phase2 value " + phase2value + " accepted for index " + phase2index + " and timestamp " + phase2timestamp);

            response = DadkvsPaxos.PhaseTwoReply.newBuilder()
                    .setPhase2Config(phase2config)
                    .setPhase2Index(phase2index)
                    .setPhase2Accepted(true)
                    .build();

            Context.current().fork().run(() -> {
                send4Learners();
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
        // for debug purposes
        System.out.println("Receive learn request: " + request);

        int learnconfig = request.getLearnconfig();
        int learnindex = request.getLearnindex();
        int learnvalue = request.getLearnvalue();
        int learntimestamp = request.getLearntimestamp(); //!FIXME: why do we need the timestamp here??


        //Save request to be processed in case a Majority of servers accepted this request
        if (learntimestamp > this.server_state.learn_messages_received.getVersion()) {
            this.server_state.learn_messages_received.setVersion(learntimestamp);
            this.server_state.learn_messages_received.setValue(1);
        } else if (learntimestamp == this.server_state.learn_messages_received.getVersion()) {
            this.server_state.learn_messages_received.setValue(this.server_state.learn_messages_received.getValue() + 1);
        }

        if (this.server_state.learn_messages_received.getValue() >= 3) {
            this.server_state.pendingRequestsForProcessing.put(learnvalue, learnindex);
            this.server_state.pendingRequestsForPaxos.remove("" + learnvalue);
            this.server_state.resetPaxosInstanceValues();
        }

        // for debug purposes
        System.out.println("Learn value " + learnvalue + " for index " + learnindex + " and timestamp " + learntimestamp);

        DadkvsPaxos.LearnReply response = DadkvsPaxos.LearnReply.newBuilder() //!FIXME: Always true???
                .setLearnconfig(learnconfig)
                .setLearnindex(learnindex)
                .setLearnaccepted(true)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    //Acceptor when accepts Phase2 sends ACCEPT to all learners
    private void send4Learners() {
        DadkvsPaxos.LearnRequest.Builder learn_request = DadkvsPaxos.LearnRequest.newBuilder();

        learn_request.setLearnconfig(this.server_state.configuration)
                .setLearnindex(this.server_state.curr_index)
                .setLearnvalue(this.server_state.last_seen_value.getValue())
                .setLearntimestamp(this.server_state.last_seen_timestamp);

        //Send request
        ArrayList<DadkvsPaxos.LearnReply> learn_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.LearnReply> learn_collector
                = new GenericResponseCollector<>(learn_responses, 5);

        // for debug purposes
        System.out.println("LEARN sending request to all learners for index: " + this.server_state.curr_index + " and timestamp: " + this.server_state.timestamp);

        // Request is sent for learners (every server)
        for (int i = 0; i < 5; i++) {
            CollectorStreamObserver<DadkvsPaxos.LearnReply> learn_observer
                    = new CollectorStreamObserver<>(learn_collector);
            this.server_state.async_stubs[i].learn(learn_request.build(), learn_observer);
        }
    }
}
