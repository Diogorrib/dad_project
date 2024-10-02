package dadkvs.server;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.util.Random;

public class DadkvsMainServiceImpl extends DadkvsMainServiceGrpc.DadkvsMainServiceImplBase {

    DadkvsServerState server_state;
    int timestamp;
    boolean freezeEnabled;
    boolean delayEnabled;

    public DadkvsMainServiceImpl(DadkvsServerState state) {
        this.server_state = state;
        this.timestamp = 0;
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

        // 1- adicionar o request à list de nao processados
        // 2- avisa o paxos loop que um request novo chegou
        // 3- <reqid, observer> para quando o paxos loop acabar
        // 4- paxos acaba (no learner) de ordenar e faz notify ao main loop
        // 5- saber a que cliente responder com o pedido ordenado (no observer)

        splitThreads(reqid);
        delay();

        VersionedValue vv = this.server_state.store.read(key);

        // ensure that request was processed before proceeding to the next one
        finishRequestProcess();

        DadkvsMain.ReadReply response = DadkvsMain.ReadReply.newBuilder()
                .setReqid(reqid).setValue(vv.getValue()).setTimestamp(vv.getVersion()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
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

        splitThreads(reqid);
        delay();

        // for debug purposes
        System.out.println("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2 + " v2 " + version2 + " wk " + writekey + " writeval " + writeval);

        this.timestamp++;
        TransactionRecord txrecord = new TransactionRecord(key1, version1, key2, version2, writekey, writeval, this.timestamp);
        boolean result = this.server_state.store.commit(txrecord);

        //Update configuration attribute on server_state
        if (writekey == 0 && result) {
            this.server_state.configuration = writeval;
        }

        // ensure that request was processed before proceeding to the next one
        finishRequestProcess();

        // for debug purposes
        System.out.println("Result is ready for request with reqid " + reqid);

        DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
                .setReqid(reqid).setAck(result).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
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
            int randomDelay = 100 + random.nextInt(9900);

            // for debug purposes
            System.out.println("delaying " + randomDelay + " milliseconds");
            try {
                Thread.sleep(randomDelay);
            } catch (InterruptedException e) {
            }
        }
    }

    private void splitThreads(int reqid) {
        Context.current().fork().run(() -> {
            this.server_state.paxos_loop.startPaxos(reqid);
        });
        this.server_state.requests_loop.waitForOrder(reqid);
    }

    private void finishRequestProcess() {
        this.server_state.paxos_loop.next_to_process++;
        System.out.println("####################################");
        for (int i = 1; i < 11; i++) {
            System.out.println("value: " + this.server_state.store.read(i).getValue());
            System.out.println("version: " + this.server_state.store.read(i).getVersion());
        }
        System.out.println("####################################");
        if (!this.server_state.pendingRequestsForProcessing.isEmpty()) {
            this.server_state.requests_loop.wakeup();
        }
    }
}
