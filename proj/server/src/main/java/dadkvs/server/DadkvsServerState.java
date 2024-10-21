package dadkvs.server;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DadkvsServerState {
    boolean         i_am_leader;
    int             debug_mode;
    int             base_port;
    int             my_id;
    int             store_size;
    KeyValueStore   store;

    MainLoop        main_loop;
    PaxosLoop       paxos_loop;
    Thread          main_loop_worker;
    Thread          paxos_loop_worker;

    int             configuration;
    int             last_seen_timestamp;
    int             last_index_i_have;
    Freeze          freeze;

    // <index, Paxos object>
    TreeMap<Integer, Paxos> paxosInstances;
    TreeMap<Integer, Paxos> ongoingPaxosInstances;

    // <reqId> -> requests to be decided order in Paxos
    ArrayList<String> pendingRequestsForPaxos;
    ArrayList<String> ongoingRequestsForPaxos;

    // <index, reqId> -> requests ready to be executed (consensus reached for these requests)
    TreeMap<Integer, Integer> pendingRequestsForProcessing;

    // <reqId, index> -> requests for which consensus has already been reached
    TreeMap<Integer, Integer> orderedRequestsByPaxos;

    // the data from pending requests, read = <reqId, [key]> or commit = <reqId, [key1, v1, key2, v2, wKey, wVal]>
    TreeMap<Integer, ArrayList<Integer>> pendingRequestsData;

    TreeMap<Integer, StreamObserver<DadkvsMain.ReadReply>> pendingRequestsReadObserver;
    TreeMap<Integer, StreamObserver<DadkvsMain.CommitReply>> pendingRequestsCommitObserver;

    Integer config_change_index;

    static final int n_servers = 5;
    ManagedChannel[] channels;
    DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;

    public DadkvsServerState(int kv_size, int port, int myself) {
        base_port = port;
        my_id = myself;
        i_am_leader = false;
        debug_mode = 0;
        store_size = kv_size;
        store = new KeyValueStore(kv_size);

        configuration = 0;
        last_seen_timestamp = -1;
        last_index_i_have = -1;
        freeze = new Freeze();

        // Used for Paxos
        paxosInstances = new TreeMap<>();
        ongoingPaxosInstances = new TreeMap<>();
        pendingRequestsForPaxos = new ArrayList<>();
        ongoingRequestsForPaxos = new ArrayList<>();
        orderedRequestsByPaxos = new TreeMap<>();

        // Requests ready to process and respective data
        pendingRequestsForProcessing = new TreeMap<>();
        pendingRequestsData = new TreeMap<>();
        pendingRequestsReadObserver = new TreeMap<>();
        pendingRequestsCommitObserver = new TreeMap<>();

        main_loop = new MainLoop(this);
        paxos_loop = new PaxosLoop(this);
        main_loop_worker = new Thread (main_loop);
        main_loop_worker.start();
        paxos_loop_worker = new Thread (paxos_loop);
        paxos_loop_worker.start();

        config_change_index = null;
    }

    public void initComms() {
        String[] targets = new String[n_servers];

        // set servers
        for (int i = 0; i < n_servers; i++) {
            int target_port = base_port + i;
            targets[i] = "localhost:" + target_port;
            System.out.printf("targets[%d] = %s%n", i, targets[i]);
        }

        // Let us use plaintext communication because we do not have certificates
        channels = new ManagedChannel[n_servers];

        for (int i = 0; i < n_servers; i++) {
            channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
        }

        async_stubs = new DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[n_servers];

        for (int i = 0; i < n_servers; i++) {
            async_stubs[i] = DadkvsPaxosServiceGrpc.newStub(channels[i]);
        }
    }

    public void terminateComms() {
        for (int i = 0; i < n_servers; i++) {
            channels[i].shutdown();
        }
    }

    synchronized private int getActualIndex() {
        int index = 0;
        while (paxosInstances.get(index) != null) {
            index++;
        }
        return index;
    }

    synchronized public int getLastLearnedIndex() {
        int index = 0;
        while (true) {
            Paxos instance = paxosInstances.get(index);
            if (instance == null || !instance.consensus_reached) break;
            index++;
        }
        return index;
    }

    // Create a new paxos instance for the next available index (pending -> ongoing)
    synchronized public void tryNextValue(int index, int timestamp) {
        if (!pendingRequestsForPaxos.isEmpty() && !paxos_loop.stop) {
            Paxos paxos = createPaxosInstance(index, configuration, timestamp);
            paxos_loop.stop = paxos.reserveValue();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(paxos::phase2);
        }
    }

    /*
    * Suppose we were executing paxos for a given reqid (old_value) with index X, but then another
    * leader executed paxos for another reqid (new_value) for the same index X. In this case
    * we will execute new_value for index X and old_value will go back to pendingRequestsForPaxos.
    */
    synchronized public void makeOldValueAvailable(int old_value, int new_value) {
        if (ongoingRequestsForPaxos.contains("" + old_value)) {
            ongoingRequestsForPaxos.remove("" + old_value);
            pendingRequestsForPaxos.add("" + old_value);
        }
        if (old_value % 100 == 0) {
            paxos_loop.stop = false;
        }
        pendingRequestsForPaxos.remove("" + new_value);
        ongoingRequestsForPaxos.add("" + new_value);
        paxos_loop.wakeup();
    }

    synchronized private void changeConfiguration(int config) {
        if (paxos_loop.stop) {
            configuration = config;
            paxos_loop.stop = false;
            paxos_loop.wakeup();
        }
    }

    // value -> reqid
    synchronized private void changeConfiguration(int value, int config) {
        // ConsoleClient has id zero, so it's requests will always be like 100, 200, ...
        if (value % 100 == 0 && configuration == config) {
            changeConfiguration(config + 1);
        }
    }

    // Paxos ended, reqid goes from ongoing -> orderedRequests, and it's added to requests ready to be processed
    synchronized public void endPaxos(Paxos paxos_instance, int config, int index, int value) {
        System.out.println("Paxos instance " + index + " ending...");
        changeConfiguration(value, config);
        pendingRequestsForPaxos.remove("" + value);
        ongoingRequestsForPaxos.remove("" + value);
        ongoingPaxosInstances.remove(index);
        orderedRequestsByPaxos.put(value, index);
        addRequestForProcessing(value, index);
        endPaxosInstance(paxos_instance);
        System.out.println("Paxos instance " + index + " ended, consensus reached for value: " + value);
    }

    synchronized private void addRequestForProcessing(int value, int index) {
        pendingRequestsForProcessing.put(index, value);
        main_loop.wakeup();
    }

    synchronized private void endPaxosInstance(Paxos paxos_instance) {
        if (!paxos_instance.consensus_reached) {
            paxos_instance.consensus_reached = true;
            paxos_instance.wakeup();
        }
        paxos_loop.wakeup();
    }

    synchronized public void addPendingRequestsForPaxos(int reqid) {
        // only add the request if it isn't already processed or being processed
        if (orderedRequestsByPaxos.get(reqid) == null
                && !pendingRequestsForPaxos.contains("" + reqid)
                && !ongoingRequestsForPaxos.contains("" + reqid)) {
            pendingRequestsForPaxos.add("" + reqid);
            paxos_loop.wakeup();
        }
    }

    synchronized public Paxos createPaxosInstance(int index, int timestamp) {
        if (paxosInstances.get(index) == null) {

            Paxos paxos = new Paxos(this, index, timestamp);

            paxosInstances.put(index, paxos);
            ongoingPaxosInstances.put(index, paxos);
        }
        return paxosInstances.get(index);
    }

    //Same as the function before, but updates the current configuration if needed
    // (used when receives phase1, phase2 or learn requests)
    synchronized public Paxos createPaxosInstance(int index, int config, int timestamp) {
        if (this.configuration < config) {
            changeConfiguration(config);
        }
        Paxos paxos = createPaxosInstance(index, timestamp);
        paxos.configuration = config;
        return paxos;
    }

}
