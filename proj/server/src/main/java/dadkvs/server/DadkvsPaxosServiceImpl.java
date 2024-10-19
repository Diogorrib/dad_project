package dadkvs.server;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Random;

public class DadkvsPaxosServiceImpl extends DadkvsPaxosServiceGrpc.DadkvsPaxosServiceImplBase {

    DadkvsServerState server_state;
    int n_servers;


    public DadkvsPaxosServiceImpl(DadkvsServerState state) {
        this.server_state = state;
        this.n_servers = DadkvsServerState.n_servers;
    }


    @Override
    public void phaseone(DadkvsPaxos.PhaseOneRequest request, StreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver) {
        if (this.server_state.freeze.enabled) return;

        // for debug purposes
        System.out.println("Receive phase1 request: " + request);

        int phase1config = request.getPhase1Config();
        int phase1index = request.getPhase1Index();
        int phase1timestamp = request.getPhase1Timestamp();
        DadkvsPaxos.PhaseOneReply response;

        Paxos paxos_instance = this.server_state.createPaxosInstance(phase1index, phase1config);
        if (phase1timestamp >= paxos_instance.last_seen_timestamp) {
            paxos_instance.last_seen_timestamp = phase1timestamp;

            // for debug purposes
            System.out.println("Phase1 accepted for index " + phase1index + " and timestamp " + phase1timestamp);

            response = DadkvsPaxos.PhaseOneReply.newBuilder()
                    .setPhase1Config(phase1config)
                    .setPhase1Index(phase1index)
                    .setPhase1Accepted(true)
                    .setPhase1Value(paxos_instance.last_seen_value.getValue())
                    .setPhase1Timestamp(paxos_instance.last_seen_value.getVersion())
                    .build();
        } else {
            // for debug purposes
            System.out.println("Phase1 rejected for index " + phase1index + " and timestamp " + phase1timestamp);

            response = DadkvsPaxos.PhaseOneReply.newBuilder()
                    .setPhase1Config(phase1config)
                    .setPhase1Index(phase1index)
                    .setPhase1Accepted(false)
                    .setPhase1Timestamp(paxos_instance.last_seen_timestamp)
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DadkvsPaxos.PhaseTwoRequest request, StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {
        if (this.server_state.freeze.enabled) return;

        // for debug purposes
        System.out.println("Receive phase two request: " + request);

        int phase2config = request.getPhase2Config();
        int phase2index = request.getPhase2Index();
        int phase2value = request.getPhase2Value();
        int phase2timestamp = request.getPhase2Timestamp();
        DadkvsPaxos.PhaseTwoReply response;

        Paxos paxos_instance = this.server_state.createPaxosInstance(phase2index, phase2config);
        if (phase2timestamp >= paxos_instance.last_seen_timestamp) {
            paxos_instance.updateValue(phase2value, phase2timestamp);

            // for debug purposes
            System.out.println("Phase2 accepted with value " + phase2value + " for index " + phase2index + " and timestamp " + phase2timestamp);

            response = DadkvsPaxos.PhaseTwoReply.newBuilder()
                    .setPhase2Config(phase2config)
                    .setPhase2Index(phase2index)
                    .setPhase2Accepted(true)
                    .build();

            Context.current().fork().run(() -> send4Learners(phase2config, phase2timestamp, phase2index, phase2value));
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
        if (this.server_state.freeze.enabled) return;

        // For debug purposes
        System.out.println("Receive learn request: " + request);

        int learnconfig = request.getLearnconfig();
        int learnindex = request.getLearnindex();
        int learnvalue = request.getLearnvalue();
        int learntimestamp = request.getLearntimestamp();

        Paxos paxos_instance = this.server_state.createPaxosInstance(learnindex, learnconfig);

        // Save request to be processed in case a Majority of servers accepted this request
        if (paxos_instance.updateLearnMessagesReceived(learntimestamp)) {
            Random random = new Random();
            int randomDelay = 2500 + random.nextInt(5000);
            try {
                Thread.sleep(randomDelay);
            } catch (InterruptedException e) {
            }
            this.server_state.endPaxos(paxos_instance, learnconfig, learnindex, learnvalue);
        }

        // For debug purposes
        System.out.println("Learn accepted with value " + learnvalue + " for index " + learnindex + " and timestamp " + learntimestamp);

        DadkvsPaxos.LearnReply response = DadkvsPaxos.LearnReply.newBuilder()
                .setLearnconfig(learnconfig)
                .setLearnindex(learnindex)
                .setLearnaccepted(true)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    // Acceptor when accepts Phase2 sends ACCEPT to all learners
    private void send4Learners(int config, int timestamp, int index, int value) {
        DadkvsPaxos.LearnRequest.Builder learn_request = DadkvsPaxos.LearnRequest.newBuilder();
        learn_request.setLearnconfig(config)
                .setLearnindex(index)
                .setLearnvalue(value)
                .setLearntimestamp(timestamp);

        // Send request
        ArrayList<DadkvsPaxos.LearnReply> learn_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.LearnReply> learn_collector
                = new GenericResponseCollector<>(learn_responses, n_servers);

        // For debug purposes
        System.out.println("Learn sending request to all acceptors for index: " + index + " and timestamp: "
                + timestamp + " with value: " + value);

        // Request is sent for learners (every server)
        for (int i = 0; i < n_servers; i++) {
            CollectorStreamObserver<DadkvsPaxos.LearnReply> learn_observer
                    = new CollectorStreamObserver<>(learn_collector);
            this.server_state.async_stubs[i].learn(learn_request.build(), learn_observer);
        }
    }
}
