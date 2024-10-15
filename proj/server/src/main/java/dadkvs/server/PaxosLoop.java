package dadkvs.server;


public class PaxosLoop implements Runnable {
    DadkvsServerState server_state;
    int curr_index;

    public PaxosLoop(DadkvsServerState state) {
        this.server_state = state;
        this.curr_index = 0;
    }

    public void run() {
        while (true) {
            this.doWork();
        }
    }

    public void doWork() {
        System.out.println("Paxos loop do work start");

        this.server_state.tryNextValue(curr_index);

        synchronized (this) {
            while (this.server_state.pendingRequestsForPaxos.isEmpty()) {
                System.out.println("Paxos loop do work: waiting");
                try {
                    wait();
                } catch (InterruptedException _) {
                }
            }
        }
        System.out.println("Paxos loop do work finish");
    }

    synchronized public void wakeup() {
        notify();
    }
}
