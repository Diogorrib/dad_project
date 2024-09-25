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
        // for debug purposes
        System.out.println("Receive sequence number request: " + request);

        int reqId = request.getReqid();
        int sequence_number = request.getSeqNumber();

        this.server_state.pendingRequests.put(reqId, sequence_number);

        this.server_state.wakeUp(); //Won't do the same thing has main loop??

        //this.server_state.main_loop.wakeup(); //!FIXME

        DadkvsSequencer.SendSeqNumberReply response = DadkvsSequencer.SendSeqNumberReply.newBuilder()
                .setReqid(reqId).setAccepted(true).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
