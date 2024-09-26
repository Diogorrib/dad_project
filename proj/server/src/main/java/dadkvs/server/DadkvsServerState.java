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
    int            sequence_number; // seq_number that leader assigns to each request
    int            curr_seq_number; // seq_number of the request server is processing
    // <reqId, SeqNumber>
    TreeMap<Integer, Integer> pendingRequests;

    ManagedChannel[] channels;
    DadkvsSequencerServiceGrpc.DadkvsSequencerServiceStub[] async_stubs;


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
    sequence_number = 0;
    curr_seq_number = 0;
    pendingRequests = new TreeMap<>();
    }

    public void initComms() {
        String[] targets = new String[5];
        sequence_number = curr_seq_number + pendingRequests.size();

        // set servers
        for (int i = 0; i < 5; i++) {
            int target_port = base_port + i;

            //if (target_port == base_port + my_id) //don't create a channel to itself
            //    continue;

            targets[i] = "localhost:" + target_port;
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

    public void terminateComms() {
        for (int i = 0; i < 5; i++) {
            channels[i].shutdown();
        }
    }

    synchronized public void waitForOrder(int reqId) {
        Integer seq_number = pendingRequests.get(reqId);
        while (seq_number == null || seq_number != curr_seq_number) {
            System.out.println("Wait for Order: waiting " + curr_seq_number);
            try {
                wait();
            } catch (InterruptedException e) {
            }
            seq_number = pendingRequests.get(reqId);
        }
        System.out.println("Wait for Order: completed");
        pendingRequests.remove(reqId);
    }

    synchronized public void wakeUp() {
        notifyAll(); // since there could be multiple requests waiting to be processed
    }
}
