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
        int reqid = request.getReqid();
        int key = request.getKey();

        if (freeze(key)) {
            System.out.println("freezed blocking read request:" + request);
            return;
        }
        // for debug purposes
        System.out.println("Receiving read request:" + request);

        delay(key);

        ArrayList<Integer> list = new ArrayList<>(List.of(key));
        this.server_state.pendingRequestsReadObserver.put(reqid, responseObserver);
        addRequestForPaxos(reqid, list);
    }

    @Override
    public void committx(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
        int reqid = request.getReqid();
        int key1 = request.getKey1();
        int version1 = request.getVersion1();
        int key2 = request.getKey2();
        int version2 = request.getVersion2();
        int writekey = request.getWritekey();
        int writeval = request.getWriteval();

        if (freeze(writekey)) {
            System.out.println("freezed blocking commit request:" + request);
            return;
        }
        // for debug purposes
        System.out.println("Receiving commit request:" + request);

        delay(writekey);

        ArrayList<Integer> list = new ArrayList<>(Arrays.asList(key1, version1, key2, version2, writekey, writeval));
        this.server_state.pendingRequestsCommitObserver.put(reqid, responseObserver);
        addRequestForPaxos(reqid, list);

        // for debug purposes
        System.out.println("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2 + " v2 " + version2 + " wk " + writekey + " writeval " + writeval);
    }

    private boolean freeze(int key) {
        if (key == 0) {
            return false;
        }

        if (this.server_state.debug_mode == 2) {
            freezeEnabled = true;
        } else if (this.server_state.debug_mode == 3) {
            freezeEnabled = false;
        }
        return freezeEnabled ;
    }

    private void delay(int key) {
        if (key == 0) {
            return;
        }

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
        System.out.println("WWWWWWWWWWWWWWWWWWWWWWWWWWWaking up Paxos Loop");
        this.server_state.paxos_loop.wakeup();
        this.server_state.main_loop.wakeup();
    }
}
