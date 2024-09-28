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
    Thread         main_loop_worker;
    int            configuration;
    int            curr_index;
    int            timestamp;
    VersionedValue last_seen_value;

    // <reqId>
    ArrayList<Integer> pendingRequestsForPaxos;

    // <reqId, index>
    TreeMap<Integer, Integer> pendingRequestsForProcessing;

    VersionedValue  learn_messages_received;
    boolean         in_paxos_instance;
    int             curr_seq_number;

    ManagedChannel[] channels;
    DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;


    public DadkvsServerState(int kv_size, int port, int myself) {
	base_port = port;
	my_id = myself;
	i_am_leader = false;
	debug_mode = 0;
	store_size = kv_size;
	store = new KeyValueStore(kv_size);
	main_loop = new MainLoop(this);
	main_loop_worker = new Thread (main_loop);
	main_loop_worker.start();
    configuration = 0;
    curr_index = 0;
    timestamp = my_id;
    last_seen_value = new VersionedValue(-1, -1);
    pendingRequestsForPaxos = new ArrayList<>();
    pendingRequestsForProcessing = new TreeMap<>();
    learn_messages_received = new VersionedValue(-1, 0);
    in_paxos_instance = false;
    curr_seq_number = 0;
    }

    public void initComms() {
        String[] targets = new String[5];

        // set servers
        for (int i = 0; i < 5; i++) {
            int target_port = base_port + i;

            // FIXME do we really need all that stubs for all replicas

            targets[i] = "localhost:" + target_port;
            System.out.printf("targets[%d] = %s%n", i, targets[i]);
        }

        // Let us use plaintext communication because we do not have certificates
        channels = new ManagedChannel[5];

        for (int i = 0; i < 5; i++) {
            channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
        }

        async_stubs = new DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[5];

        for (int i = 0; i < 5; i++) {
            async_stubs[i] = DadkvsPaxosServiceGrpc.newStub(channels[i]);
        }
    }

    public void terminateComms() {
        for (int i = 0; i < 5; i++) {
            channels[i].shutdown();
        }
    }

    public boolean inConfiguration() {
        return my_id >= configuration && my_id < configuration + 3;
    }

    public void updateValue(int value, int timestamp) {
        last_seen_value.setValue(value);
        last_seen_value.setVersion(timestamp);
    }
}
