package dadkvs.server;

import dadkvs.DadkvsPaxos;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;

import java.util.ArrayList;
import java.util.Iterator;

public class Paxos {
    DadkvsServerState server_state;

    private static final int n_servers = 5;
    private static final int n_acceptors = 3;
    private static final int responses_needed = 2; // Majority of acceptors (2 of 3)

    int             index;
    int             timestamp;
    boolean         consensus_reached;
    int             configuration;
    int             last_seen_timestamp;
    VersionedValue  last_seen_value;            // (value, timestamp)
    VersionedValue  learn_messages_received;    // (n_responses, timestamp)

    public Paxos(DadkvsServerState state, int index) {
        this.server_state = state;
        this.index = index;
        this.timestamp = state.my_id;
        this.consensus_reached = false;
        this.configuration = state.configuration;
        this.last_seen_timestamp = 0;
        this.last_seen_value = new VersionedValue(-1, -1);
        this.learn_messages_received = new VersionedValue(0, -1);
    }

    synchronized public void wakeup() {
        notify();
    }

    synchronized public void startPaxos() {
        while (!this.consensus_reached) {
            //I'm a proposer and leader
            if (toProposeValues()) {
                proposePaxos();
                if (this.consensus_reached) {
                    return;
                }
            }
            try {
                wait();
            } catch (InterruptedException _) {
            }
        }
    }

    private void proposePaxos() {
        boolean phasetwo_completed = false;

        while (!phasetwo_completed) {
            boolean phaseone_completed = false;
            if (toGiveUpFromThisInstance()) {
                return;
            }
            while (!phaseone_completed) {
                if (toGiveUpFromThisInstance()) {
                    return;
                }

                phaseone_completed = phase1();
            }
            // for debug purposes
            System.out.println("Phase1 completed");

            phasetwo_completed = phase2();
        }
        // for debug purposes
        System.out.println("Phase2 completed");
    }

    private boolean inConfiguration() {
        return (this.server_state.my_id >= this.configuration
                && this.server_state.my_id < this.configuration + n_acceptors);
    }

    // If I'm proposer and leader
    private boolean toProposeValues() {
        return this.server_state.i_am_leader && inConfiguration();
    }

    private boolean toGiveUpFromThisInstance() {
        return this.consensus_reached || !toProposeValues();
    }

    private void increaseTimestamp(int timestamp) {
        this.timestamp = (timestamp / n_servers + 1) * n_servers + this.server_state.my_id;
    }

    synchronized public void updateValue(int new_value, int timestamp) {
        int old_value = this.last_seen_value.getValue();
        if (old_value > 0 && new_value != old_value) {
            this.server_state.makeOldValueAvailable(old_value, new_value);
        }
        this.last_seen_value.setValue(new_value);
        this.last_seen_value.setVersion(timestamp);
    }

    synchronized public void checkOldValue() {
        if (this.last_seen_value.getValue() <= 0) {
            String reqid = this.server_state.pendingRequestsForPaxos.getFirst();
            this.server_state.pendingRequestsForPaxos.remove(reqid);
            this.server_state.ongoingRequestsForPaxos.add(reqid);
            this.last_seen_value.setValue(Integer.parseInt(reqid));
        }
    }

    private boolean phase1() {
        DadkvsPaxos.PhaseOneRequest.Builder phaseone_request = DadkvsPaxos.PhaseOneRequest.newBuilder();
        phaseone_request.setPhase1Config(this.configuration)
                .setPhase1Index(this.index)
                .setPhase1Timestamp(this.timestamp);

        //Send request
        ArrayList<DadkvsPaxos.PhaseOneReply> phaseone_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseone_collector
                = new GenericResponseCollector<>(phaseone_responses, n_acceptors);

        // for debug purposes
        System.out.println("Phase1 sending request to all acceptors for index: "
                + this.index + " and timestamp: " + this.timestamp + "\nWaiting for "
                + responses_needed + " responses from acceptors");

        // Request is only sent for acceptors
        for (int i = this.configuration; i < this.configuration + n_acceptors; i++) {
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
                int timestamp = phaseone_reply.getPhase1Timestamp();

                //Phase 1 rejected by one of the acceptors
                if (!phaseone_reply.getPhase1Accepted()) {
                    increaseTimestamp(timestamp);
                    // for debug purposes
                    System.out.println("Phase1 acceptor rejected. Increase timestamp to: " + this.timestamp
                            + " and try again");
                    return false;
                }

                int value = phaseone_reply.getPhase1Value();
                //Got accepted value from previous phase 2, update last_seen_value
                if (timestamp > this.last_seen_value.getVersion()
                        && this.server_state.orderedRequestsByPaxos.get(value) == null) {
                    updateValue(value, timestamp);

                    // for debug purposes
                    System.out.println("Phase1 acceptor already accepted value: "
                            + value + " with timestamp: " + timestamp);
                }
            }
            return true;
        } else {
            System.out.println("Phase1 ERROR");
            return false;
        }
    }

    private boolean phase2() {
        DadkvsPaxos.PhaseTwoRequest.Builder phasetwo_request = DadkvsPaxos.PhaseTwoRequest.newBuilder();

        int value = this.last_seen_value.getValue();
        updateValue(value, this.timestamp);

        phasetwo_request.setPhase2Config(this.configuration)
                .setPhase2Index(this.index)
                .setPhase2Value(value)
                .setPhase2Timestamp(this.timestamp);

        //Send request
        ArrayList<DadkvsPaxos.PhaseTwoReply> phasetwo_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phasetwo_collector
                = new GenericResponseCollector<>(phasetwo_responses, n_acceptors);

        // for debug purposes
        System.out.println("Phase2 sending request to all acceptors for index: " + this.index + " and timestamp: "
                + this.timestamp + " with value: " + value + "\nWaiting for " + responses_needed
                + " responses from acceptors");

        // Request is only sent for acceptors
        for (int i = this.configuration; i < this.configuration + n_acceptors; i++) {
            CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> phasetwo_observer
                    = new CollectorStreamObserver<>(phasetwo_collector);
            this.server_state.async_stubs[i].phasetwo(phasetwo_request.build(), phasetwo_observer);
        }

        phasetwo_collector.waitForTarget(responses_needed);

        return processPhase2Replies(phasetwo_responses);
    }

    private boolean processPhase2Replies(ArrayList<DadkvsPaxos.PhaseTwoReply> phasetwo_responses) {
        if (phasetwo_responses.size() >= responses_needed) {
            Iterator<DadkvsPaxos.PhaseTwoReply> phasetwo_iterator = phasetwo_responses.iterator();
            for (int i = 0; i < responses_needed; i++) {
                DadkvsPaxos.PhaseTwoReply phasetwo_reply = phasetwo_iterator.next();

                //Phase 2 rejected by one of the acceptors
                if (!phasetwo_reply.getPhase2Accepted()) {
                    increaseTimestamp(this.timestamp);
                    // for debug purposes
                    System.out.println("Phase2 acceptor rejected. Increase timestamp to: " + this.timestamp
                            + " and try again");
                    return false;
                }
            }
            return true;
        } else {
            System.out.println("Phase2 ERROR");
            return false;
        }
    }
}
