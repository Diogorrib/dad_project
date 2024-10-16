package dadkvs.server;

public class Freeze {

    boolean enabled;
    boolean configuration_change;

    public Freeze() {
        this.enabled = false;
        this.configuration_change = false;
    }

    synchronized public void freeze(int reqid) {
        while (enabled || configuration_change) {
            System.out.println("freezing request: " + reqid);
            try {
                wait();
            } catch (InterruptedException _) {
            }
            if (!enabled || !configuration_change) {
                System.out.println("unfreezing request: " + reqid);
            }
        }
    }

    synchronized public void wakeup() {
        notifyAll();
    }

}
