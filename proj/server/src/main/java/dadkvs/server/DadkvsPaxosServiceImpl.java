
package dadkvs.server;


import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.stub.StreamObserver;

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
            && phase1timestamp >= this.server_state.timestamp) {
            this.server_state.timestamp = phase1timestamp;

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
                    .setPhase1Timestamp(this.server_state.timestamp)
                    .build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DadkvsPaxos.PhaseTwoRequest request, StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {
        // for debug purposes
        System.out.println("Receive phase two request: " + request);

    }

    @Override
    public void learn(DadkvsPaxos.LearnRequest request, StreamObserver<DadkvsPaxos.LearnReply> responseObserver) {
        // for debug purposes
        System.out.println("Receive learn request: " + request);

    }

}
