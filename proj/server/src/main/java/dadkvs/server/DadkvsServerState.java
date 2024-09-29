package dadkvs.server;

import dadkvs.DadkvsSequencerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

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
    // <reqId, SeqNumber>
    TreeMap<Integer, Integer> pending_requests;

    SequencerOrder sequencer_order;

    ManagedChannel[] channels;
    DadkvsSequencerServiceGrpc.DadkvsSequencerServiceStub[] async_stubs;
    static final int n_servers = 5;


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
    pending_requests = new TreeMap<>();
    sequencer_order = new SequencerOrder(this);
    }

    public void initComms() {
        String[] targets = new String[n_servers];
        sequencer_order.sequence_number = sequencer_order.curr_seq_number + pending_requests.size();

        // set servers
        for (int i = 0; i < n_servers; i++) {
            int target_port = base_port + i;

            //if (target_port == base_port + my_id) //don't create a channel to itself
            //    continue;

            targets[i] = "localhost:" + target_port;
            System.out.printf("targets[%d] = %s%n", i, targets[i]);
        }

        // Let us use plaintext communication because we do not have certificates
        channels = new ManagedChannel[n_servers];

        for (int i = 0; i < n_servers; i++) {
            channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
        }

        async_stubs = new DadkvsSequencerServiceGrpc.DadkvsSequencerServiceStub[n_servers];

        for (int i = 0; i < n_servers; i++) {
            async_stubs[i] = DadkvsSequencerServiceGrpc.newStub(channels[i]);
        }
    }

    public void terminateComms() {
        for (int i = 0; i < n_servers; i++) {
            channels[i].shutdown();
        }
    }
}
