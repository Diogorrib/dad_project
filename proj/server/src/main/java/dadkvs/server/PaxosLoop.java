package dadkvs.server;

public class PaxosLoop implements Runnable {
    DadkvsServerState server_state;
    Paxos paxos;

    public PaxosLoop(DadkvsServerState state) {
        this.server_state = state;
        this.paxos = new Paxos(state);
    }

    public void run() {
        while (true) {
            this.doWork();
        }
    }

    synchronized public void doWork() {
        System.out.println("Paxos loop do work start");

        if(!this.server_state.pendingRequestsForPaxos.isEmpty())
            this.paxos.startPaxos();

        while (this.server_state.pendingRequestsForPaxos.isEmpty()) {
            System.out.println("Paxos loop do work: waiting");
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
        System.out.println("Paxos loop do work finish");
    }

    synchronized public void wakeup() {
        notify();
    }
}
