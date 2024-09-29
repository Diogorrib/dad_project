package dadkvs.server;

import dadkvs.DadkvsSequencer;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.Context;

import java.util.ArrayList;

public class SequencerOrder {
    DadkvsServerState server_state;
    int            sequence_number; // seq_number that leader assigns to each request
    int            curr_seq_number; // seq_number of the request server is processing

    public SequencerOrder(DadkvsServerState state){
        this.server_state = state;
        this.sequence_number = 0;
        this.curr_seq_number = 0;
    }

    public void getOrder(int reqId) {
        if (this.server_state.i_am_leader) {
            Context.current().fork().run(() -> {
                this.server_state.sequencer_order.sendOrder(reqId);
            });
        }
        // the leader also waits since it also receives the order from itself
        this.server_state.sequencer_order.waitForOrder(reqId);
    }

    private void sendOrder(int reqid) {
        DadkvsSequencer.SendSeqNumberRequest.Builder sequence_number_request = DadkvsSequencer.SendSeqNumberRequest.newBuilder();

        sequence_number_request.setReqid(reqid).setSeqNumber(this.sequence_number);

        // for debug purposes
        System.out.println("Sent to all servers seqNumber: " +  this.sequence_number + " assigned to reqId: " + reqid);


        //Send request
        ArrayList<DadkvsSequencer.SendSeqNumberReply> sequence_number_responses = new ArrayList<DadkvsSequencer.SendSeqNumberReply>();
        GenericResponseCollector<DadkvsSequencer.SendSeqNumberReply> sequence_number_collector
                = new GenericResponseCollector<DadkvsSequencer.SendSeqNumberReply>(sequence_number_responses, 5);

        for (int i = 0; i < 5; i++) {
            CollectorStreamObserver<DadkvsSequencer.SendSeqNumberReply> sequence_number_observer = new CollectorStreamObserver<DadkvsSequencer.SendSeqNumberReply>(sequence_number_collector);
            this.server_state.async_stubs[i].sendseqnumber(sequence_number_request.build(), sequence_number_observer);
        }

        this.sequence_number++;
    }

    synchronized private void waitForOrder(int reqId) {
        Integer seq_number = this.server_state.pending_requests.get(reqId);
        while (seq_number == null || seq_number != curr_seq_number) {
            System.out.println("Wait for Order with curr_seq_number:  " + curr_seq_number);
            System.out.println("SeqNumber vs CurrentSeqNumber:" + seq_number + " " + curr_seq_number);
            try {
                wait();
            } catch (InterruptedException e) {
            }
            seq_number = this.server_state.pending_requests.get(reqId);
        }
        System.out.println("Wait for Order: completed");
        this.server_state.pending_requests.remove(reqId);
    }

    synchronized public void wakeUp() {
        notifyAll(); // since there could be multiple requests waiting to be processed
    }
}