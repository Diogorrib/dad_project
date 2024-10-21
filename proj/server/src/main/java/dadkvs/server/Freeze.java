package dadkvs.server;

public class Freeze {

    boolean enabled;                // for debug mode freeze

    public Freeze() {
        this.enabled = false;
    }

    synchronized public void freeze(int reqid) {
        while (enabled) {
            System.out.println("freezing request: " + reqid);
            try {
                wait();
            } catch (InterruptedException _) {
            }
            if (!enabled) {
                System.out.println("unfreezing request: " + reqid);
            }
        }
    }

    synchronized public void wakeup() {
        notifyAll();
    }

}
