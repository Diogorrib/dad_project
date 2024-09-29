package dadkvs.server;


import dadkvs.DadkvsSequencer;
import dadkvs.DadkvsSequencerServiceGrpc;
import io.grpc.stub.StreamObserver;

public class DadkvsSequencerServiceImpl extends DadkvsSequencerServiceGrpc.DadkvsSequencerServiceImplBase {

    DadkvsServerState server_state;

    public DadkvsSequencerServiceImpl(DadkvsServerState state) {
        this.server_state = state;
    }

    @Override
    public void sendseqnumber(DadkvsSequencer.SendSeqNumberRequest request, StreamObserver<DadkvsSequencer.SendSeqNumberReply> responseObserver) {

        int reqId = request.getReqid();
        int sequence_number = request.getSeqNumber();

        this.server_state.pending_requests.put(reqId, sequence_number);

        // for debug purposes
        System.out.println("Request with reqId: " + reqId + " has sequence number (by leader): " + sequence_number);

        this.server_state.sequencer_order.wakeUp();

        DadkvsSequencer.SendSeqNumberReply response = DadkvsSequencer.SendSeqNumberReply.newBuilder()
                .setReqid(reqId).setAccepted(true).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
