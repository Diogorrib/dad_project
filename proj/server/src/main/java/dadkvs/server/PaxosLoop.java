package dadkvs.server;

import dadkvs.DadkvsPaxos;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class PaxosLoop {
    DadkvsServerState server_state;

    private static final int n_acceptors = 3;
    private static final int responses_needed = 2; // Majority of acceptors (2 of 3)

    private static final Logger logger = Logger.getLogger(PaxosLoop.class.getName());

    int            curr_index;
    int            timestamp;
    int            last_seen_timestamp;
    boolean        in_paxos_instance;
    VersionedValue last_seen_value;

    List<Integer> learn_messages_received; //List of 3 elements (n_responses, timestamp, Index)
    int            next_to_process;         //next index to be processed

    /*
    static {
        // Set a custom formatter to include the method name
        for (Handler handler : Logger.getLogger("").getHandlers()) {
            handler.setFormatter(new SimpleFormatter() {
                private static final String format = "[%1$tF %1$tT] [%2$s] %4$s %5$s %n";

                @Override
                public synchronized String format(LogRecord lr) {
                    return String.format(format,
                            lr.getMillis(),
                            lr.getSourceClassName() + "." + lr.getSourceMethodName(),
                            lr.getLoggerName(),
                            lr.getLevel().getLocalizedName(),
                            lr.getMessage()
                    );
                }
            });
        }
    }
    */

    public PaxosLoop(DadkvsServerState state) {
        this.server_state = state;
        curr_index = 0;
        next_to_process = 0;
        timestamp = state.my_id;
        last_seen_timestamp = 0;
        in_paxos_instance = false;
        last_seen_value = new VersionedValue(-1, -1);
        learn_messages_received = new ArrayList<>(Arrays.asList(0, -1, 0));

    }

    private void waitBackoff(BackOff backoff) {
        long backoff_delay = backoff.calculateBackoffDelay();
        // for debug purposes
        System.out.println("Back off: " + backoff_delay + " milliseconds");
        try {
            Thread.sleep(backoff_delay);
        } catch (InterruptedException e) {
        }
    }

    synchronized public void startPaxos(int reqid) {
        if (this.server_state.orderedRequestsByPaxos.get(reqid) != null) {
            if (this.server_state.pendingRequestsForPaxos.isEmpty()) {
                return;
            }
        } else {
            this.server_state.pendingRequestsForPaxos.add("" + reqid);
        }

        //waitPreviousConsensus();
        this.in_paxos_instance = true;

        while (this.in_paxos_instance) {
            //I'm a proposer and leader
            if (this.server_state.iAmProposer()) {
                proposePaxos(reqid);
                if (!this.in_paxos_instance || !this.server_state.iAmProposer()) { //FIXME: OR in orderedRequests
                    return;
                }
            }
            try {
                logger.info("Waiting");
                wait(); //FIXME: All are stuck here (if proposePaxos() with success return; ??)
            } catch (InterruptedException e) {
            }
        }
        //notify();   // only one instance of paxos will start
    }

    public void proposePaxos(int reqid) {
        boolean phasetwo_completed = false;

        while (!phasetwo_completed) {
            boolean phaseone_completed = false;
            BackOff backoff = new BackOff();
            if (!this.in_paxos_instance || !this.server_state.iAmProposer()) {
                return;
            }
            while (!phaseone_completed) {
                if (!this.in_paxos_instance || !this.server_state.iAmProposer()) {
                    return;
                }

                phaseone_completed = phase1();

                // wait before trying with higher leader value
                if(!phaseone_completed)
                    waitBackoff(backoff);
            }
            // for debug purposes
            System.out.println("Phase1 completed");

            phasetwo_completed = phase2();

            if(!phasetwo_completed)
                waitBackoff(backoff);
        }
        // for debug purposes
        System.out.println("Phase2 completed");
    }


    private boolean phase1() {
        DadkvsPaxos.PhaseOneRequest.Builder phaseone_request = DadkvsPaxos.PhaseOneRequest.newBuilder();

        phaseone_request.setPhase1Config(this.server_state.configuration)
                .setPhase1Index(this.curr_index)
                .setPhase1Timestamp(this.timestamp);

        //Send request
        ArrayList<DadkvsPaxos.PhaseOneReply> phaseone_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseone_collector
                = new GenericResponseCollector<>(phaseone_responses, n_acceptors);

        // for debug purposes
        System.out.println("Phase1 sending request to all acceptors for index: " + this.curr_index + " and timestamp: " + this.timestamp);

        // Request is only sent for acceptors
        for (int i = this.server_state.configuration; i < this.server_state.configuration + n_acceptors; i++) {
            CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phaseone_observer
                    = new CollectorStreamObserver<>(phaseone_collector);
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
                    
                    this.server_state.increaseTimestamp(timestamp);
                    // for debug purposes
                    System.out.println("Phase1 Acceptor rejected. Increase timestamp to: " + this.timestamp);
                    return false; // try again with new timestamp
                }

                //Got accepted value from previous phase 2, update last_seen_value
                if (timestamp > this.last_seen_value.getVersion()){
                    int value = phaseone_reply.getPhase1Value();
                    // for debug purposes
                    System.out.println("Phase1 already accepted value: " + value + " with timestamp: " + timestamp);
                    this.server_state.updateValue(value, timestamp);
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
        if (value == -1) {
            value = Integer.parseInt(this.server_state.pendingRequestsForPaxos.getFirst());
        }

        this.server_state.updateValue(value, this.timestamp);

        phasetwo_request.setPhase2Config(this.server_state.configuration)
                .setPhase2Index(this.curr_index)
                .setPhase2Value(value)
                .setPhase2Timestamp(this.timestamp);

        //Send request
        ArrayList<DadkvsPaxos.PhaseTwoReply> phasetwo_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> phasetwo_collector
                = new GenericResponseCollector<>(phasetwo_responses, n_acceptors);

        // for debug purposes
        System.out.println("Phase2 sending request to all acceptors for index: " + this.curr_index + " and timestamp: " + this.timestamp);

        // Request is only sent for acceptors
        for (int i = this.server_state.configuration; i < this.server_state.configuration + n_acceptors; i++) {
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
                    this.server_state.increaseTimestamp(this.timestamp);
                    // for debug purposes
                    System.out.println("Phase2 Acceptor rejected. Increase timestamp to: " + this.timestamp);
                    return false; // try again with new timestamp
                }
            }
            return true;
        } else {
            System.out.println("Phase2 ERROR");
            return false;
        }
    }

    public void resetPaxosInstanceValues() {
        this.curr_index++;
        this.timestamp = this.server_state.my_id;
        this.last_seen_timestamp = 0;
        this.last_seen_value = new VersionedValue(-1, -1);
        this.learn_messages_received = new ArrayList<>(Arrays.asList(0, -1, this.curr_index));
        this.in_paxos_instance = false;
    }


    synchronized private void waitPreviousConsensus() {
        while (this.in_paxos_instance) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
    }

    synchronized public void wakeup() {
        notify();
    }
}
