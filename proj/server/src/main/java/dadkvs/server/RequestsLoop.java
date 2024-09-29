package dadkvs.server;

public class RequestsLoop {
    DadkvsServerState server_state;

    public RequestsLoop(DadkvsServerState state) {
        this.server_state = state;
    }

    synchronized public void waitForOrder(int reqid) {
        Integer seq_number = this.server_state.pendingRequestsForProcessing.get(reqid);
        while (seq_number == null || seq_number != this.server_state.paxos_loop.next_to_process) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
            seq_number = this.server_state.pendingRequestsForProcessing.get(reqid);
        }
        System.out.println("Wait for Order: completed");
        this.server_state.pendingRequestsForProcessing.remove(reqid);
    }

    synchronized public void wakeup() {
        notifyAll();    // since there could be multiple requests waiting to be processed
    }
}