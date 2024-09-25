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
    int            sequence_number;
    // <reqId, SeqNumber>
    TreeMap<Integer, Integer> pendingRequests = new TreeMap<>();

    ManagedChannel[] channels;
    DadkvsSequencerServiceGrpc.DadkvsSequencerServiceStub[] async_stubs;


    public DadkvsServerState(int kv_size, int port, int myself) {
	base_port = port;
	my_id = myself;
	i_am_leader = false;
	debug_mode = 0;
	store_size = kv_size;
    sequence_number = 1;
	store = new KeyValueStore(kv_size);
	main_loop = new MainLoop(this);
	main_loop_worker = new Thread (main_loop);
	main_loop_worker.start();
    }

    void initComms() {
        String[] targets = new String[5];

        // set servers
        for (int i = 0; i < 5; i++) {
            int target_port = base_port + i;

            //if (target_port == base_port + my_id) //don't create a channel to itself
            //    continue;

            targets[i] = "";
            targets[i] = "localhost" + ":" + target_port;
            System.out.printf("targets[%d] = %s%n", i, targets[i]);
        }

        // Let us use plaintext communication because we do not have certificates
        channels = new ManagedChannel[5];

        for (int i = 0; i < 5; i++) {
            channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
        }

        async_stubs = new DadkvsSequencerServiceGrpc.DadkvsSequencerServiceStub[5];

        for (int i = 0; i < 5; i++) {
            async_stubs[i] = DadkvsSequencerServiceGrpc.newStub(channels[i]);
        }
    }

    synchronized public void waitForOrder(int reqId, int currentSeqNumber) {
        Integer seq_number = this.pendingRequests.get(reqId);
        while (seq_number == null || seq_number != currentSeqNumber) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
            seq_number = this.pendingRequests.get(reqId);
        }
    }

    synchronized public void wakeUp() {
        notify();
    }
}
