package dadkvs.server;

/* these imported classes are generated by the contract */

import dadkvs.DadkvsConsole;
import dadkvs.DadkvsConsoleServiceGrpc;

import io.grpc.stub.StreamObserver;

public class DadkvsConsoleServiceImpl extends DadkvsConsoleServiceGrpc.DadkvsConsoleServiceImplBase {


    DadkvsServerState server_state;

    public DadkvsConsoleServiceImpl(DadkvsServerState state) {
        this.server_state = state;
    }

    @Override
    public void setleader(DadkvsConsole.SetLeaderRequest request, StreamObserver<DadkvsConsole.SetLeaderReply> responseObserver) {
        // for debug purposes
        System.out.println(request);

        boolean response_value = true;
        boolean i_am_leader = request.getIsleader();
        manageComms(this.server_state.i_am_leader, i_am_leader);
        this.server_state.i_am_leader = i_am_leader;

        // for debug purposes
        System.out.println("I am the leader = " + this.server_state.i_am_leader);

        this.server_state.main_loop.wakeup();

        DadkvsConsole.SetLeaderReply response = DadkvsConsole.SetLeaderReply.newBuilder()
                .setIsleaderack(response_value).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void setdebug(DadkvsConsole.SetDebugRequest request, StreamObserver<DadkvsConsole.SetDebugReply> responseObserver) {
        // for debug purposes
        System.out.println(request);

        boolean response_value = true;

        this.server_state.debug_mode = request.getMode();
        this.server_state.main_loop.wakeup();

        // for debug purposes
        System.out.println("Setting debug mode to = " + this.server_state.debug_mode);

        DadkvsConsole.SetDebugReply response = DadkvsConsole.SetDebugReply.newBuilder()
                .setAck(response_value).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void manageComms(boolean i_am_leader_old, boolean i_am_leader_new) {
        if (!i_am_leader_old && i_am_leader_new) {
            server_state.initComms(); //create stubs to speak with other servers
        } else if (i_am_leader_old && !i_am_leader_new) {
            server_state.terminateComms();
        }
    }
}
