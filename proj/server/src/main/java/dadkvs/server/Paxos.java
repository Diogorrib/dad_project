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
    int             index; // Associated to this Paxos Instance
    int             timestamp;
    boolean         consensus_reached;
    int             configuration;
    VersionedValue  last_seen_value;            // (value, timestamp)
    VersionedValue  learn_messages_received;    // (n_responses, timestamp)

    final Object value_lock = new Object();
    final Object messages_lock = new Object();

    public Paxos(DadkvsServerState state, int index, int timestamp) {
        this.server_state = state;
        this.index = index;
        this.timestamp = timestamp;
        this.consensus_reached = false;
        this.configuration = state.configuration;
        this.last_seen_value = new VersionedValue(-1, -1);
        this.learn_messages_received = new VersionedValue(0, -1);
    }

    synchronized public void wakeup() {
        notify();
    }

    // Update the last seen value, received from other server (the work already done by others)
    public void updateValue(int new_value, int timestamp) {
        synchronized (this.value_lock) {
            int old_value = this.last_seen_value.getValue();
            if (new_value != old_value) {
                if (old_value > 0) {
                    this.server_state.makeOldValueAvailable(old_value, new_value);
                }
                this.last_seen_value.setValue(new_value);
            }
            this.last_seen_value.setVersion(timestamp);
        }
    }

    public boolean reserveValue() {
        synchronized (this.value_lock) {
            // if no value was accepted, reserve value from pending requests from client
            if (this.last_seen_value.getValue() <= 0) {
                String reqid = this.server_state.pendingRequestsForPaxos.getFirst();
                this.server_state.pendingRequestsForPaxos.remove(reqid);
                this.server_state.ongoingRequestsForPaxos.add(reqid);
                this.last_seen_value.setValue(Integer.parseInt(reqid));
            }
            return (this.last_seen_value.getValue() % 100 == 0);
        }
    }

    public void updateValueFromPreviousLeader(int value, int timestamp) {
        //Got accepted value from previous phase 2, update last_seen_value
        synchronized (this.value_lock) {
            if (timestamp > this.last_seen_value.getVersion()
                    && this.server_state.orderedRequestsByPaxos.get(value) == null) {

                updateValue(value, timestamp);
                // for debug purposes
                System.out.println("Phase1 (index: " + this.index + ") acceptor already accepted value: "
                        + value + " with timestamp: " + timestamp);
            }
        }
    }

    // To keep count of the messages received from learn requests
    public boolean updateLearnMessagesReceived(int timestamp) {
        synchronized (this.messages_lock) {
            if (this.consensus_reached) return false; // avoid doing paxos more than one time

            if (timestamp > this.learn_messages_received.getVersion()) {
                this.learn_messages_received.setValue(1);
                this.learn_messages_received.setVersion(timestamp);

            } else if (timestamp == this.learn_messages_received.getVersion()) {
                this.learn_messages_received.setValue(this.learn_messages_received.getValue() + 1);

                // Save request to be processed in case a Majority of servers accepted this request
                return this.learn_messages_received.getValue() == responses_needed;
            }
            return false;
        }
    }

    public void phase2() {
        DadkvsPaxos.PhaseTwoRequest.Builder phasetwo_request = DadkvsPaxos.PhaseTwoRequest.newBuilder();
        int value;
        synchronized (this.value_lock) {
            value = this.last_seen_value.getValue();
            updateValue(value, this.timestamp);
        }

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

        processPhase2Replies(phasetwo_responses);
    }

    private void processPhase2Replies(ArrayList<DadkvsPaxos.PhaseTwoReply> phasetwo_responses) {

        if (phasetwo_responses.size() >= responses_needed) {
            Iterator<DadkvsPaxos.PhaseTwoReply> phasetwo_iterator = phasetwo_responses.iterator();

            for (int i = 0; i < responses_needed; i++) {
                DadkvsPaxos.PhaseTwoReply phasetwo_reply = phasetwo_iterator.next();

                //Phase 2 rejected by one of the acceptors
                if (!phasetwo_reply.getPhase2Accepted()) {
                    this.server_state.paxos_loop.prepareAgain();
                    // for debug purposes
                    System.out.println("Phase2 (index: " + this.index + ") acceptor rejected. Increase timestamp to: "
                            + this.timestamp + " and try again");
                    break;
                }
            }
        } else {
            System.out.println("Phase2 ERROR");
        }
    }
}
