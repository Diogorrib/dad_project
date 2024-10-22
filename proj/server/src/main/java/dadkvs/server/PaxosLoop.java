package dadkvs.server;


import dadkvs.DadkvsPaxos;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PaxosLoop implements Runnable {
    DadkvsServerState server_state;
    boolean stop;   // used during reconfiguration
    int timestamp;
    int curr_index;
    boolean phaseone_completed;
    private static final int n_servers = 5;
    private static final int n_acceptors = 3;
    private static final int responses_needed = 2; // Majority of acceptors (2 of 3)

    public PaxosLoop(DadkvsServerState state) {
        this.server_state = state;
        this.stop = false;
        this.timestamp = this.server_state.my_id;
        this.phaseone_completed = false;
    }

    public void run() {
        while (true) {
            this.doWork();
        }
    }

    public void doWork() {
        System.out.println("Paxos loop do work start");

        if (toProposeValues()) {
            if (!phaseone_completed) {
                phaseone_completed = phase1();
            } else {
                this.server_state.tryNextValue(this.curr_index, this.timestamp);
            }
        }

        synchronized (this) {
            while (!toProposeValues()) {
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

    private boolean inConfiguration() {
        return (this.server_state.my_id >= this.server_state.configuration
                && this.server_state.my_id < this.server_state.configuration + n_acceptors);
    }

    // If I'm proposer and leader
    private boolean toProposeValues() {
        return this.server_state.i_am_leader && inConfiguration() && !this.stop &&
                (!this.server_state.pendingRequestsForPaxos.isEmpty() ||
                  this.server_state.ongoingPaxosInstances.containsKey(this.curr_index));
    }

    public void prepareAgain() {
        this.phaseone_completed = false;
        increaseTimestamp(this.server_state.last_seen_timestamp);
    }

    private boolean phase1() {
        this.curr_index  = this.server_state.getLastLearnedIndex();
        DadkvsPaxos.PhaseOneRequest.Builder phaseone_request = DadkvsPaxos.PhaseOneRequest.newBuilder();
        phaseone_request.setPhase1Config(this.server_state.configuration)
                .setPhase1Index(this.curr_index)
                .setPhase1Timestamp(this.timestamp);

        //Send request
        ArrayList<DadkvsPaxos.PhaseOneReply> phaseone_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseone_collector
                = new GenericResponseCollector<>(phaseone_responses, n_acceptors);

        // for debug purposes
        System.out.println("Phase1 sending request to all acceptors for index: "
                + this.curr_index + " and timestamp: " + this.timestamp + "\nWaiting for "
                + responses_needed + " responses from acceptors");

        // Request is only sent for acceptors
        for (int i = this.server_state.configuration; i < this.server_state.configuration + n_acceptors; i++) {
            CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phaseone_observer =
                    new CollectorStreamObserver<>(phaseone_collector);
            this.server_state.async_stubs[i].phaseone(phaseone_request.build(), phaseone_observer);
        }

        phaseone_collector.waitForTarget(responses_needed);

        //Process responses for phase 1
        return processPhase1Replies(phaseone_responses);
    }

    private boolean processPhase1Replies(ArrayList<DadkvsPaxos.PhaseOneReply> phaseone_responses) {

        if (phaseone_responses.size() >= responses_needed) {
            Iterator<DadkvsPaxos.PhaseOneReply> phaseone_iterator = phaseone_responses.iterator();

            for (int i = 0; i < responses_needed; i++) {

                DadkvsPaxos.PhaseOneReply phaseone_reply = phaseone_iterator.next();
                List<Integer> phase1timestamps = phaseone_reply.getPhase1TimestampList();

                //Phase 1 rejected by one of the acceptors
                if (!phaseone_reply.getPhase1Accepted()) {
                    //ACCEPTORS: IF REJECTED -> SEND ONLY THE HIGHEST TIMESTAMP
                    increaseTimestamp(phase1timestamps.getFirst());
                    // for debug purposes
                    System.out.println("Phase1 rejected by acceptor. Increase timestamp to: "
                            + this.timestamp + " and try again");
                    return false;
                }

                List<Integer> phase1indexes = phaseone_reply.getPhase1IndexList();
                List<Integer> phase1values = phaseone_reply.getPhase1ValueList();

                // Create all instances with the information received
                for (int j = 0; j < phase1indexes.size(); j++) {
                    Paxos paxos = this.server_state.createPaxosInstance(phase1indexes.get(j), this.timestamp);
                    paxos.updateValueFromPreviousLeader(phase1values.get(j), phase1timestamps.get(j));
                }
                if (!phase1indexes.isEmpty()) {
                    int last = phase1indexes.getLast();
                    if (this.server_state.last_index_i_have < last)
                        this.server_state.last_index_i_have = last;
                }
            }
            return true;

        } else {
            System.out.println("Phase1 ERROR");
            return false;
        }
    }

    private void increaseTimestamp(int timestamp) {
        this.timestamp = (timestamp / n_servers + 1) * n_servers + this.server_state.my_id;
    }
}
