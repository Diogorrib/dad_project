package dadkvs.server;

import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.TreeMap;

public class DadkvsServerState {
    boolean        i_am_leader;
    int            debug_mode;
    int            base_port;
    int            my_id;
    int            store_size;
    KeyValueStore  store;
    MainLoop       main_loop;
    PaxosLoop      paxos_loop;
    RequestsLoop   requests_loop;
    Thread         main_loop_worker;
    int            configuration; // for paxos

    // <reqId>
    ArrayList<String> pendingRequestsForPaxos;

    // <reqId, index>
    TreeMap<Integer, Integer> pendingRequestsForProcessing;

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
        paxos_loop = new PaxosLoop(this);
        requests_loop = new RequestsLoop(this);
        main_loop = new MainLoop(this);
        main_loop_worker = new Thread (main_loop);
        main_loop_worker.start();
        configuration = 0;
        pendingRequestsForPaxos = new ArrayList<>();
        pendingRequestsForProcessing = new TreeMap<>();
    }

    public void initComms() {
        String[] targets = new String[n_servers];

        // set servers
        for (int i = 0; i < n_servers; i++) {
            int target_port = base_port + i;

            // FIXME do we really need all that stubs for all replicas

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

    public boolean inConfiguration() {
        return my_id >= configuration && my_id < configuration + 3;
    }

    // If I'm proposer and leader
    public boolean iAmProposer() {
        return i_am_leader && inConfiguration();
    }

    public void updateValue(int value, int timestamp) {
        this.paxos_loop.last_seen_value.setValue(value);
        this.paxos_loop.last_seen_value.setVersion(timestamp);
    }

    public void increaseTimestamp(int timestamp) {
        this.paxos_loop.timestamp = (timestamp / n_servers + 1) * n_servers + my_id;
    }

    public void resetPaxosInstance() {
        paxos_loop.resetPaxosInstanceValues();
        paxos_loop.wakeup();
        requests_loop.wakeup();
    }
}
