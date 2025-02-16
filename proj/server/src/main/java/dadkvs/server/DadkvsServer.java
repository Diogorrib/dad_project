package dadkvs.server;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.concurrent.TimeUnit;


public class DadkvsServer {

    static DadkvsServerState server_state;
    private static Server server;

    public static void main(String[] args) throws Exception {
        final int kvsize = 1000;

        System.out.println(DadkvsServer.class.getSimpleName());

        // Print received arguments.
        System.out.printf("Received %d arguments%n", args.length);
        for (int i = 0; i < args.length; i++) {
            System.out.printf("arg[%d] = %s%n", i, args[i]);
        }

        // Check arguments.
        if (args.length < 2) {
            System.err.println("Argument(s) missing!");
            System.err.printf("Usage: java %s baseport replica-id%n", Server.class.getName());
            return;
        }

        int base_port = Integer.parseInt(args[0]);
        int my_id = Integer.parseInt(args[1]);

        server_state = new DadkvsServerState(kvsize, base_port, my_id);

        int port = base_port + my_id;

        final BindableService service_impl = new DadkvsMainServiceImpl(server_state);
        final BindableService console_impl = new DadkvsConsoleServiceImpl(server_state);
        final BindableService paxos_impl = new DadkvsPaxosServiceImpl(server_state);

        // Create a new server to listen on port.
        server = ServerBuilder.forPort(port).addService(service_impl).addService(console_impl).addService(paxos_impl).build();

        server_state.initComms();
        // Start the server.
        server.start();
        // Server threads are running in the background.
        System.out.println("Server started");

        // Do not exit the main thread. Wait until server is terminated.
        server.awaitTermination();
    }

    // Used for debug mode 1
    public static void simulateCrash() {
        if (server != null) {
            try {
                server_state.terminateComms();
                server.shutdown();
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                server.shutdownNow();
            }
            System.exit(0);
        }
    }
}
