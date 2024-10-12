package dadkvs.server;

import io.grpc.Context;

public class PaxosLoop implements Runnable {
    DadkvsServerState server_state;
    int curr_index;
    //Paxos paxos;

    public PaxosLoop(DadkvsServerState state) {
        this.server_state = state;
        this.curr_index = 0;
        //this.paxos = new Paxos(state);
    }

    public void run() {
        while (true) {
            this.doWork();
        }
    }

    synchronized public void doWork() {
        System.out.println("Paxos loop do work start");

        if (!this.server_state.pendingRequestsForPaxos.isEmpty()) {
            System.out.println("INDEX = " + this.curr_index);
            Paxos paxos = this.server_state.createPaxosInstance(curr_index);
            paxos.startPaxos();
        }

        while (this.server_state.pendingRequestsForPaxos.isEmpty()) {
            System.out.println("Paxos loop do work: waiting");
            try {
                wait();
            } catch (InterruptedException _) {
            }
        }
        System.out.println("Paxos loop do work finish");
    }

    synchronized public void wakeup() {
        notify();
    }
}
