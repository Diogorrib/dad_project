package dadkvs.server;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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
    Freeze          freeze;

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

    //private static final Logger logger = Logger.getLogger(Paxos.class.getName());

    /*static {
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
    }*/

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

    private int getActualIndex() {
        int index = 0;
        while (paxosInstances.get(index) != null) {
            index++;
        }
        return index;
    }

    synchronized public void tryNextValue() {
        if (!pendingRequestsForPaxos.isEmpty() && !paxos_loop.stop) {
            Paxos paxos = createPaxosInstance(getActualIndex());
            paxos.checkOldValue();
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(paxos::startPaxos);
        }
    }

    synchronized public void makeOldValueAvailable(int old_value, int new_value) {
        ongoingRequestsForPaxos.remove("" + old_value);
        pendingRequestsForPaxos.add("" + old_value);
        pendingRequestsForPaxos.remove("" + new_value);
        ongoingRequestsForPaxos.add("" + new_value);
        paxos_loop.wakeup();
    }

    synchronized public void endPaxos(Paxos paxos_instance, int config, int index, int value) {
        System.out.println("ENDING paxos for index: " + index);
        changeConfiguration(value, config, index);
        pendingRequestsForPaxos.remove("" + value);
        ongoingRequestsForPaxos.remove("" + value);
        ongoingPaxosInstances.remove(index);
        orderedRequestsByPaxos.put(value, index);
        addRequestForProcessing(value, index);
        nextPaxosInstance(paxos_instance);
    }

    // value -> reqid
    synchronized private void changeConfiguration(int value, int config, int index) {
        // ConsoleClient has id zero, so it's requests will always be like 100, 200, ...
        if (value % 100 == 0 && configuration == config) {
            stopPaxos(index);
            resetNextInstances(index + 1);
            configuration = config + 1;
            paxos_loop.stop = false;
            freeze.configuration_change = false;
            paxos_loop.wakeup();
            freeze.wakeup();
        }
    }

    synchronized private void stopPaxos(int index) {
        paxos_loop.stop = true;
        freeze.configuration_change = true;
        Collection<Paxos> aux = ongoingPaxosInstances.values(); // FIXME: Maybe deepcopy???
        for(Paxos paxos : aux) {    // FIXME: concurrent modification exception
            while (index < paxos.index && !paxos.consensus_reached) {
                try {
                    wait();
                } catch (InterruptedException _) {
                }
            }
        }
    }

    synchronized private void resetNextInstances(int index) {
        ArrayList<String> temp = pendingRequestsForPaxos;   // FIXME: Maybe deepcopy???
        pendingRequestsForPaxos = new ArrayList<>();
        int actual_index = getActualIndex();
        for (int i = index; i < actual_index; i++) {
            System.out.println("#################################\t" + i + " vs " + actual_index);
            int reqid = pendingRequestsForProcessing.get(i);    // FIXME: null ptr exception
            //Was decided in the older configuration
            pendingRequestsForProcessing.remove(i);
            orderedRequestsByPaxos.remove(reqid);

            pendingRequestsForPaxos.add("" + reqid);
            paxosInstances.remove(i);
        }
        pendingRequestsForPaxos.addAll(temp);
    }

    synchronized private void nextPaxosInstance(Paxos paxos_instance) {
        if (!paxos_instance.consensus_reached) {
            paxos_instance.consensus_reached = true;
            paxos_instance.wakeup();
            notify();
        }
        paxos_loop.wakeup();
    }

    synchronized private void addRequestForProcessing(int value, int index) {
        pendingRequestsForProcessing.put(index, value);
        main_loop.wakeup();
    }

    synchronized public void addPendingRequestsForPaxos(int reqid) {
        if (orderedRequestsByPaxos.get(reqid) == null
                && !pendingRequestsForPaxos.contains("" + reqid)
                && !ongoingRequestsForPaxos.contains("" + reqid)) {
            pendingRequestsForPaxos.add("" + reqid);
        }
    }

    synchronized public Paxos createPaxosInstance(int index) {
        if (paxosInstances.get(index) == null) {

            Paxos paxos = new Paxos(this, index);

            paxosInstances.put(index, paxos);
            ongoingPaxosInstances.put(index, paxos);
        }
        return paxosInstances.get(index);
    }

    synchronized public Paxos createPaxosInstance(int index, int config) {
        if (this.configuration < config) {
            configuration = config;
        }
        return createPaxosInstance(index);
    }

}
