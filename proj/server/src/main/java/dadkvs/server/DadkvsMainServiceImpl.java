package dadkvs.server;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class DadkvsMainServiceImpl extends DadkvsMainServiceGrpc.DadkvsMainServiceImplBase {

    DadkvsServerState server_state;
    boolean freezeEnabled;
    boolean delayEnabled;

    public DadkvsMainServiceImpl(DadkvsServerState state) {
        this.server_state = state;
        this.delayEnabled = false;
        this.freezeEnabled = false;
    }

    @Override
    public void read(DadkvsMain.ReadRequest request, StreamObserver<DadkvsMain.ReadReply> responseObserver) {
        if (freeze()) {
            System.out.println("freezed blocking read request:" + request);
            return;
        }
        // for debug purposes
        System.out.println("Receiving read request:" + request);

        int reqid = request.getReqid();
        int key = request.getKey();

        delay();

        ArrayList<Integer> list = new ArrayList<>(List.of(key));
        this.server_state.pendingRequestsReadObserver.put(reqid, responseObserver);
        addRequestForPaxos(reqid, list);
    }

    @Override
    public void committx(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
        if (freeze()) {
            System.out.println("freezed blocking commit request:" + request);
            return;
        }
        // for debug purposes
        System.out.println("Receiving commit request:" + request);

        int reqid = request.getReqid();
        int key1 = request.getKey1();
        int version1 = request.getVersion1();
        int key2 = request.getKey2();
        int version2 = request.getVersion2();
        int writekey = request.getWritekey();
        int writeval = request.getWriteval();

        delay();

        ArrayList<Integer> list = new ArrayList<>(Arrays.asList(key1, version1, key2, version2, writekey, writeval));
        this.server_state.pendingRequestsCommitObserver.put(reqid, responseObserver);
        addRequestForPaxos(reqid, list);

        // for debug purposes
        System.out.println("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2 + " v2 " + version2 + " wk " + writekey + " writeval " + writeval);
    }

    private boolean freeze() {
        if (this.server_state.debug_mode == 2) {
            freezeEnabled = true;
        } else if (this.server_state.debug_mode == 3) {
            freezeEnabled = false;
        }
        return freezeEnabled;
    }

    private void delay() {
        if (this.server_state.debug_mode == 4) {
            delayEnabled = true;
        } else if (this.server_state.debug_mode == 5) {
            delayEnabled = false;
        }

        if (delayEnabled) {
            Random random = new Random();
            int randomDelay = 100 + random.nextInt(2900);

            // for debug purposes
            System.out.println("delaying " + randomDelay + " milliseconds");
            try {
                Thread.sleep(randomDelay);
            } catch (InterruptedException e) {
            }
        }
    }

    private void addRequestForPaxos(int reqid, ArrayList<Integer> requestList) {
        if (this.server_state.orderedRequestsByPaxos.get(reqid) == null) {
            this.server_state.pendingRequestsForPaxos.add("" + reqid);
        }
        this.server_state.pendingRequestsData.put(reqid, requestList);
        this.server_state.paxos_loop.wakeup();
        this.server_state.main_loop.wakeup();
    }
}
